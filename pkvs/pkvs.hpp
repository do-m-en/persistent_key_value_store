#ifndef PKVS_HPP_INCLUDED
#define PKVS_HPP_INCLUDED

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <chrono>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include "detail/memtable.hpp"
#include "detail/sstables.hpp"

namespace pkvs
{
  class pkvs_t
  {
  public:
    pkvs_t( size_t instance_no, size_t memtable_memory_footprint_eviction_threshold );

    // contract: assert( key.empty() == false && key.size() < 256 );
    seastar::future<std::optional<std::string>> get_item( std::string_view key );
    // contract: assert( key.empty() == false && key.size() < 256 );
    seastar::future<> insert_item( std::string_view key, std::string_view value );
    // contract: assert( key.empty() == false && key.size() < 256 );
    seastar::future<> delete_item( std::string_view key );
    seastar::future<std::set<std::string>> sorted_keys();

    // takes care of writes of data to disk etc. and should be called periodically
    seastar::future<> housekeeping();
    size_t approximate_memtable_memory_footprint() const
    {
      return approximate_memtable_memory_footprint_;
    }

  private:
    memtable_t memtable_;
    size_t memtable_memory_footprint_eviction_threshold_;
    size_t approximate_memtable_memory_footprint_; // in bytes
    std::chrono::time_point<std::chrono::system_clock> last_persist_time_;
    bool has_dirty_ = false;
    sstables_t sstables_;
  };
}

#endif // PKVS_HPP_INCLUDED