//  Copyright 2024 Domen Vrankar
//
//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

#include "sstables.hpp"

#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>

#include <csignal>
#include <iostream>
#include <map>
#include <ranges>
#include <span>
#include <sstream>

using namespace pkvs;

namespace
{
  std::string file_name_from_key( std::string_view key )
  {
    // two hashes to remove the risk of a hash collision
    size_t hash = std::hash<std::string_view>{}( key );
    auto reverse = key | std::views::reverse;
    std::string reverse_key{ reverse.begin(), reverse.end() };
    size_t reverse_hash = std::hash<std::string>{}( reverse_key );

    return std::to_string( hash ) + '_' + std::to_string( reverse_hash );
  }

  enum class entry_type
  {
    tombstone,
    value
  };

  constexpr size_t entry_size = sizeof( uint64_t ) + 256 + sizeof( uint32_t );
}

seastar::future<sstables_t> sstables_t::make( std::filesystem::path base_path )
{
  auto path = base_path / "sstables";
  auto values_dir = path / "values";

  if( co_await seastar::file_exists( path.native() ) == false )
    co_await seastar::make_directory( path.native() );

  if( co_await seastar::file_exists( values_dir.native() ) == false )
    co_await seastar::make_directory( values_dir.native() );

  std::vector<unsigned long> sstables;

  for( auto const& entry : std::filesystem::directory_iterator( path) )
  {
    // skip values directory
    // TODO assert that it's the expected directory named "values"
    if( entry.is_directory() )
      continue;

    // TODO check for files corrutption and if there are unexpected files/directories present
    sstables.push_back( std::stoul( entry.path().stem() ) );
  }

  std::ranges::sort( sstables );

  co_return sstables_t{ path, std::move( sstables ) };
}

sstables_t::sstables_t
(
  std::filesystem::path base_path,
  std::vector<unsigned long>&& sstables
)
  : base_path_{ base_path }
  , sstables_{ std::forward< std::vector<unsigned long> >( sstables) }
{}

seastar::future<std::optional<std::string>> sstables_t::get_item( std::string_view key )
{
  for( bool found = false; auto current : sstables_ | std::views::reverse )
  {
    auto in_ss_table_file =
      co_await seastar::open_file_dma
      (
        ( base_path_ / std::to_string( current ) ).native(),
        seastar::open_flags::ro
      );
    auto in_sstable_stream = seastar::make_file_input_stream( in_ss_table_file );

    uint32_t type;

    co_await
      [ & ] -> seastar::future<>
      {
        while( true )
        {
          auto read = co_await in_sstable_stream.read_up_to( entry_size );

          if( read.size() == 0 )
            co_return;
          else if( read.size() != entry_size )
          {
            std::raise( SIGKILL );

            throw
              std::runtime_error
              (
                "sstables file corruption detected in " +
                ( base_path_ / std::to_string( current ) ).native()
              );
          }

          uint64_t size = *reinterpret_cast< uint64_t const* >( read.get() );
          std::string_view found_key{ read.get() + sizeof( uint64_t ), size };

          if( found_key == key )
          {
            type =
              *reinterpret_cast< uint32_t const* >
              (
                read.get() + read.size() - sizeof( uint32_t )
              );
            found = true;

            co_return;
          }
        }
      }()
      .finally( [ & ]{ return in_sstable_stream.close(); } );

    if( found )
    {
      if( static_cast<entry_type>( type ) == entry_type::tombstone )
        break;

      auto in_file =
        co_await seastar::open_file_dma
        (
          ( base_path_ / "values" / file_name_from_key( key ) ).native(),
          seastar::open_flags::ro
        );
      auto in_stream = seastar::make_file_input_stream( in_file );

      std::stringstream input;


      co_await
        [ & ] -> seastar::future<>
        {
          while( true )
          {
            auto read = co_await in_stream.read();

            if( read.size() == 0 )
              co_return;

            input.write( read.get(), read.size() );
          }
        }()
        .finally( [ & ]{ return in_stream.close(); } );

      co_return input.str();
    }
  }

  co_return std::nullopt;
}

seastar::future<std::set<std::string>> sstables_t::sorted_keys()
{
  std::map< std::string, entry_type > keys;

  for( auto current : sstables_ )
  {
    auto in_ss_table_file =
      co_await seastar::open_file_dma
      (
        ( base_path_ / std::to_string( current ) ).native(),
        seastar::open_flags::ro
      );
    auto in_sstable_stream = seastar::make_file_input_stream( in_ss_table_file );

    co_await
      [ & ] -> seastar::future<>
      {
        while( true )
        {
          auto read = co_await in_sstable_stream.read_up_to( entry_size );

          if( read.size() == 0 )
            co_return;
          else if( read.size() != entry_size )
          {
            std::raise( SIGKILL );

            throw
              std::runtime_error
              (
                "sstables file corruption detected in " +
                ( base_path_ / std::to_string( current ) ).native()
              );
          }

          uint64_t size = *reinterpret_cast< uint64_t const* >( read.get() );
          std::string found_key{ read.get() + sizeof( uint64_t ), size };
          uint32_t type =
            *reinterpret_cast< uint32_t const* >
            (
              read.get() + read.size() - sizeof( uint32_t )
            );

          keys[ found_key ] = static_cast<entry_type>( type );
        }
      }()
      .finally( [ & ]{ return in_sstable_stream.close(); } );
  }

  std::set< std::string > return_keys;

  for( auto const& item : keys )
  {
    if( item.second == entry_type::value )
      return_keys.insert( item.first );
  }

  co_return return_keys;
}

seastar::future<> sstables_t::store( std::span< sstable_item_t > items )
{
  for( auto const& item : items )
  {
    if( item.value != std::nullopt )
    {
      auto out_file =
        co_await seastar::open_file_dma
        (
          ( base_path_ / "values" / file_name_from_key( item.key ) ).native(),
          seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate
        );
      auto out_stream = co_await seastar::make_file_output_stream( out_file );

      co_await
        [&] -> seastar::future<>
        {
          auto const& value = item.value.value();

          co_await out_stream.write( value.data(), value.size() );
        }()
        .finally(
          seastar::coroutine::lambda(
            [ & ] -> seastar::future<>
            {
              co_await out_stream.flush();
              co_await out_stream.close();
            }));
    }
  }

  unsigned long next = sstables_.empty() ? 0 : sstables_.back() + 1;

  auto out_ss_table_file =
    co_await seastar::open_file_dma
    (
      ( base_path_ / std::to_string( next ) ).native(),
      seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate
    );
  auto out_sstable_stream = co_await seastar::make_file_output_stream( out_ss_table_file );

  co_await
    [&] -> seastar::future<>
    {
      for( auto const& item : items )
      {
        std::string fill( 256 - item.key.size(), ' ' );

        uint32_t type =
          static_cast<uint32_t>(
            item.value == std::nullopt ?
            entry_type::tombstone :
            entry_type::value );

        uint64_t size = item.key.size();

        co_await out_sstable_stream.write( reinterpret_cast<const char*>(&size), sizeof( size ) );
        co_await out_sstable_stream.write( item.key.c_str(), size );
        co_await out_sstable_stream.write( fill.c_str(), 256 - item.key.size() );
        co_await out_sstable_stream.write( reinterpret_cast<const char*>(&type), sizeof( type ) );
      }
    }()
    .finally(
      seastar::coroutine::lambda(
        [ & ] -> seastar::future<>
        {
          co_await out_sstable_stream.flush();
          co_await out_sstable_stream.close();
        }));

  sstables_.push_back( next );
}

seastar::future<> sstables_t::try_merge_oldest()
{
  // TODO implement
  return seastar::make_ready_future<>();
}