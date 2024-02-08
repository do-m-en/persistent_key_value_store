#include "pkvs.hpp"

#include <cassert>
#include <filesystem>

using namespace pkvs;

pkvs_t::pkvs_t( size_t instance_no )
  : instance_no_{ instance_no }
{
  std::filesystem::create_directories
  (
    std::filesystem::current_path() / "pkvs_data" / std::to_string( instance_no_ )
  );
}

seastar::future<std::optional<std::string>> pkvs_t::get_item( std::string_view key ) const
{
  assert( key.empty() == false && key.size() < 256 );

  auto& index = memtable_.get< key_index >();

  if
  (
    auto found = index.find( entry_t{ key } );
    found != index.end() && (*found).type != entry_type_t::tombstone
  )
  {
    // TODO support for not loaded values
    return seastar::make_ready_future<std::optional<std::string>>( (*found).content );
  }

  return seastar::make_ready_future<std::optional<std::string>>( std::nullopt );
}

seastar::future<> pkvs_t::insert_item( std::string_view key, std::string_view value )
{
  assert( key.empty() == false && key.size() < 256 );

  auto& index = memtable_.get< key_index >();

  if( auto found = index.find( entry_t{ key } ); found != index.end() )
    index.replace( found, entry_t{ key, value, true } );
  else
    index.insert( entry_t{ key, value, true } );

  return seastar::make_ready_future<>();
}

seastar::future<> pkvs_t::delete_item( std::string_view key )
{
  assert( key.empty() == false && key.size() < 256 );

  auto& index = memtable_.get< key_index >();

  if( auto found = index.find( entry_t{ key } ); found != index.end() )
    index.replace( found, entry_t::make_tombstone( key ) );
  else
    index.insert( entry_t::make_tombstone( key ) );

  return seastar::make_ready_future<>();
}

seastar::future<std::set<std::string>> pkvs_t::sorted_keys()
{
  std::set<std::string> keys;

  // TODO support for not loaded keys
  for( auto const& item : memtable_.get< key_index >() )
  {
    if( item.type != entry_type_t::tombstone )
      keys.insert( item.key );
  }

  return seastar::make_ready_future<std::set<std::string>>( keys );
}