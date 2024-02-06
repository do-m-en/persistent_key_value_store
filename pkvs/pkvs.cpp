#include "pkvs.hpp"

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
  return seastar::make_ready_future<std::optional<std::string>>("");
}

seastar::future<bool> pkvs_t::insert_item( std::string_view key, std::string_view value )
{
  return seastar::make_ready_future<bool>(true);
}

seastar::future<bool> pkvs_t::delete_item( std::string_view key )
{
  return seastar::make_ready_future<bool>(true);
}