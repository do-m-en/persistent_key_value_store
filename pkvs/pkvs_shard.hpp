#ifndef PKVS_SHARD_HPP_INCLUDED
#define PKVS_SHARD_HPP_INCLUDED

#include <seastar/core/future.hh>

#include <string_view>
#include <vector>

namespace pkvs
{
  inline constexpr size_t pkvs_segments_count = 256;

  size_t key_to_shard_no( std::string_view key )
  {
    size_t hash = std::hash<std::string_view>{}( key );

    return (hash % pkvs_segments_count) % seastar::smp::count;
  }

  class pkvs_shard
  {
  public:
    seastar::future<> run()
    {
      return seastar::make_ready_future<>();
    }

    seastar::future<> stop()
    {
      return seastar::make_ready_future<>();
    }

    seastar::future<std::optional<std::string>> get_item( std::string_view key )
    {
      std::cerr << "running on " << seastar::this_shard_id() << "\n";
      return seastar::make_ready_future<std::optional<std::string>>("");
    }

    // return true if new item, false on update of existing item
    seastar::future<bool> insert_item( std::string_view key, std::string_view value )
    {
      return seastar::make_ready_future<bool>(true);
    }

    seastar::future<bool> delete_item( std::string_view key )
    {
      return seastar::make_ready_future<bool>(true);
    }

  private:
    //std::vector<>
  };
}

#endif // PKVS_SHARD_HPP_INCLUDED