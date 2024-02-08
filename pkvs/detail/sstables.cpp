#include "sstables.hpp"

#include <ranges>

using namespace pkvs;

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
  // TODO implement
  return seastar::make_ready_future<std::optional<std::string>>();
}

seastar::future<std::set<std::string>> sstables_t::sorted_keys()
{
  // TODO implement
  return seastar::make_ready_future<std::set<std::string>>();
}

seastar::future<> sstables_t::store( std::span< sstable_item_t > items )
{
  // TODO implement
  return seastar::make_ready_future<>();
}

seastar::future<> sstables_t::try_merge_oldest()
{
  // TODO implement
  return seastar::make_ready_future<>();
}