//  Copyright 2024 Domen Vrankar
//
//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

#include "pkvs.hpp"

#include <seastar/core/seastar.hh>

#include <cassert>
#include <filesystem>

using namespace pkvs;

seastar::future< pkvs_t > pkvs_t::make
(
  size_t instance_no,
  size_t memtable_memory_footprint_eviction_threshold
)
{
  auto root_pksv_data_dir = std::filesystem::current_path() / "pkvs_data";
  auto root_instance_dir = root_pksv_data_dir / std::to_string( instance_no );

  if( co_await seastar::file_exists( root_instance_dir.native() ) == false )
    co_await seastar::make_directory( root_instance_dir.native() );

  co_return
    pkvs_t
    {
      instance_no,
      memtable_memory_footprint_eviction_threshold,
      co_await sstables_t::make( root_instance_dir )
    };
}

pkvs_t::pkvs_t
(
  size_t instance_no,
  size_t memtable_memory_footprint_eviction_threshold,
  sstables_t&& sstables_
)
  : memtable_{ std::make_unique< memtable_t >() }
  , memtable_memory_footprint_eviction_threshold_{ memtable_memory_footprint_eviction_threshold }
  , last_persist_time_{ std::chrono::system_clock::now() }
  , sstables_{ std::forward< sstables_t >( sstables_ ) }
{}

seastar::future<std::optional<std::string>> pkvs_t::get_item( std::string_view key )
{
  assert( key.empty() == false && key.size() < 256 );

  auto& index = memtable_->get< key_index >();

  if
  (
    auto found = index.find( entry_t{ key } );
    found != index.end() && found->type != entry_type_t::tombstone
  )
  {
    index.modify( found, []( auto& item ){ item.bump_last_access_time(); });

    co_return found->content;
  }
  else
  {
    auto item = co_await sstables_.get_item( key );

    if( item != std::nullopt )
    {
      auto& index = memtable_->get< key_index >();

      index.insert( entry_t{ key, item.value(), false } );
      approximate_memtable_memory_footprint_ += key.size() + item.value().size();

      co_return item;
    }
  }

  co_return std::nullopt;
}

void pkvs_t::insert_item( std::string_view key, std::string_view value )
{
  assert( key.empty() == false && key.size() < 256 );

  has_dirty_ = true;

  auto& index = memtable_->get< key_index >();

  if( auto found = index.find( entry_t{ key } ); found != index.end() )
  {
    approximate_memtable_memory_footprint_ -= found->content.size();
    index.replace( found, entry_t{ key, value, true } );
  }
  else
  {
    index.insert( entry_t{ key, value, true } );
    approximate_memtable_memory_footprint_ += key.size();
  }

  approximate_memtable_memory_footprint_ += value.size();
}

void pkvs_t::delete_item( std::string_view key )
{
  assert( key.empty() == false && key.size() < 256 );

  has_dirty_ = true;

  auto& index = memtable_->get< key_index >();

  if( auto found = index.find( entry_t{ key } ); found != index.end() )
  {
    approximate_memtable_memory_footprint_ -= found->content.size();
    index.replace( found, entry_t::make_tombstone( key ) );
  }
  else
  {
    index.insert( entry_t::make_tombstone( key ) );
    approximate_memtable_memory_footprint_ += key.size();
  }
}

seastar::future<std::set<std::string>> pkvs_t::sorted_keys()
{
  // FIXME race condition because of co_await (housekeeping can already evict
  //       some keys while we're reading and then they are missing in memtable_)
  //       prevent the race... introduce semaphor or something
  std::set<std::string> keys = co_await sstables_.sorted_keys();

  for( auto const& item : memtable_->get< key_index >() )
  {
    if( item.type == entry_type_t::tombstone )
      keys.erase( item.key );
    else
      keys.insert( item.key );
  }

  co_return keys;
}

seastar::future<> pkvs_t::housekeeping()
{
  using namespace std::literals;

  if
  (
    approximate_memtable_memory_footprint_ > memtable_memory_footprint_eviction_threshold_ ||
    std::chrono::system_clock::now() > last_persist_time_ + 20s
  )
  {
    if( has_dirty_ )
    {
      std::vector<sstable_item_t> items;

      auto& index = memtable_->get< key_index >();

      for( auto it = index.begin(); it != index.end(); ++it )
      {
        auto const& item = *it;

        if( item.dirty )
        {
          if( item.type == entry_type_t::tombstone )
            items.emplace_back( item.key, std::nullopt );
          else
            items.emplace_back( item.key, item.content );

          index.modify( it, []( auto& item ){ item.dirty = false; } );
        }
      }

      has_dirty_ = false;

      co_await sstables_.store( items );
    }

    while( approximate_memtable_memory_footprint_ > memtable_memory_footprint_eviction_threshold_ )
    {
      auto& index = memtable_->get< last_accessed_index >();

      assert( index.begin() != index.end() );

      auto first = index.begin();

      approximate_memtable_memory_footprint_ -= first->key.size() + first->content.size();

      memtable_->erase( memtable_->iterator_to( *first ) );
    }
  }
}