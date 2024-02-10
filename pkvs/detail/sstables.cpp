#include "sstables.hpp"

#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>

#include <csignal>
#include <iostream>
#include <map>
#include <ranges>
#include <span>

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

sstables_t::sstables_t( std::filesystem::path base_path )
  : base_path_{ base_path / "sstables" }
{
  std::filesystem::create_directories( base_path_ / "values" );

  for( auto const& entry : std::filesystem::directory_iterator( base_path_) )
  {
    // skip values directory
    // TODO assert that it's the expected directory named "values"
    if( entry.is_directory() )
      continue;

    // TODO check for files corrutption and if there are unexpected files/directories present
    sstables_.push_back( std::stoul( entry.path().stem() ) );
  }

  std::ranges::sort( sstables_ );
}

seastar::future<std::optional<std::string>> sstables_t::get_item( std::string_view key )
{
  // TODO implement using seastar file io

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

      std::ifstream value_file
      {
        base_path_ / "values" / file_name_from_key( key )
      };
      value_file.seekg(0, std::ios::end);
      size_t size = value_file.tellg();
      std::optional<std::string> buffer{ std::string( size, ' ' ) };
      value_file.seekg(0);
      value_file.read(buffer.value().data(), size);
      value_file.close();

      co_return buffer;
    }
  }

  co_return std::nullopt;
}

seastar::future<std::set<std::string>> sstables_t::sorted_keys()
{
  // TODO implement using seastar file io

  std::map< std::string, entry_type > keys;

  for( auto current : sstables_ )
  {
    std::ifstream file
    {
      base_path_ / std::to_string( current ),
      std::ios::binary
    };

    std::string buffer( 256, ' ' );

    file.seekg (0, std::ios::end);
    auto length = file.tellg();
    file.seekg (0, std::ios::beg);

    while( file.tellg() < length )
    {
      uint64_t size;
      file.read( reinterpret_cast<char*>( &size ), sizeof( size ) );
      file.read( buffer.data(), 256 );
      uint32_t type;
      file.read( reinterpret_cast<char*>( &type ), sizeof( type ) );

      keys[ buffer.substr( 0, size ) ] = static_cast<entry_type>( type );
    }

    file.close();
  }

  std::set< std::string > return_keys;

  for( auto const& item : keys )
  {
    if( item.second == entry_type::value )
      return_keys.insert( item.first );
  }

  return seastar::make_ready_future<std::set<std::string>>( return_keys );
}

seastar::future<> sstables_t::store( std::span< sstable_item_t > items )
{
  // TODO implement using seastar file io

  for( auto const& item : items )
  {
    if( item.value != std::nullopt )
    {
      std::ofstream file
      {
        base_path_ / "values" / file_name_from_key( item.key ),
        std::ios::trunc
      };
      file << item.value.value();
      file.close();
    }
  }

  unsigned long next = sstables_.empty() ? 0 : sstables_.back() + 1;

  std::ofstream file
  {
    base_path_ / std::to_string( next ),
    std::ios::binary | std::ios::trunc
  };

  for( auto const& item : items )
  {
    std::string fill( 256 - item.key.size(), ' ' );

    uint32_t type =
      static_cast<uint32_t>(
        item.value == std::nullopt ?
        entry_type::tombstone :
        entry_type::value );

    uint64_t size = item.key.size();
    file.write( reinterpret_cast<const char*>(&size), sizeof( size ) );
    file.write( item.key.c_str(), size );
    file.write( fill.c_str(), 256 - item.key.size() );
    file.write( reinterpret_cast<const char*>(&type), sizeof( type ) );
  }

  file.close();

  sstables_.push_back( next );

  return seastar::make_ready_future<>();
}

seastar::future<> sstables_t::try_merge_oldest()
{
  // TODO implement
  return seastar::make_ready_future<>();
}