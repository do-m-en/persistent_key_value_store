#include "pkvs.hpp"

#include <cassert>
#include <filesystem>

using namespace pkvs;

pkvs_t::pkvs_t( size_t instance_no )
  : sstables_{ std::filesystem::current_path() / "pkvs_data" / std::to_string( instance_no ) }
{}

seastar::future<std::optional<std::string>> pkvs_t::get_item( std::string_view key )
{
  assert( key.empty() == false && key.size() < 256 );

  auto& index = memtable_.get< key_index >();

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
      auto& index = memtable_.get< key_index >();

      index.insert( entry_t{ key, item.value(), false } );

      co_return item;
    }
  }

  co_return std::nullopt;
}

seastar::future<> pkvs_t::insert_item( std::string_view key, std::string_view value )
{
  assert( key.empty() == false && key.size() < 256 );

  auto& index = memtable_.get< key_index >();

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

  return seastar::make_ready_future<>();
}

seastar::future<> pkvs_t::delete_item( std::string_view key )
{
  assert( key.empty() == false && key.size() < 256 );

  auto& index = memtable_.get< key_index >();

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

  return seastar::make_ready_future<>();
}

seastar::future<std::set<std::string>> pkvs_t::sorted_keys()
{
  // FIXME race condition because of co_await (housekeeping can already evict
  //       some keys while we're reading and then they are missing in memtable_)
  //       prevent the race... introduce semaphor or something
  std::set<std::string> keys = co_await sstables_.sorted_keys();

  for( auto const& item : memtable_.get< key_index >() )
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
  return seastar::make_ready_future<>();
}