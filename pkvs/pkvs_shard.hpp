//  Copyright 2024 Domen Vrankar
//
//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

#ifndef PKVS_SHARD_HPP_INCLUDED
#define PKVS_SHARD_HPP_INCLUDED

#include <seastar/core/future.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <cassert>
#include <string_view>
#include <vector>

#include "pkvs.hpp"

namespace pkvs
{
  // amount of pkvs instances into which the key hash space should be split
  inline constexpr size_t pkvs_segments_count = 256;

  size_t key_to_segment_no( std::string_view key )
  {
    size_t hash = std::hash<std::string_view>{}( key );

    return (hash % pkvs_segments_count);
  }

  size_t key_to_shard_no( std::string_view key )
  {
    return key_to_segment_no( key ) % seastar::smp::count;
  }

  // class for taking care of pkvs instances that are assigned to a single shard
  // shard handles every n-th pkvs instance where n is mod of seastar::smp::count
  // offset by current shard id all the way to pkvs_segments_count
  class pkvs_shard
  {
  public:
    seastar::future<> run( size_t memtable_memory_footprint_eviction_threshold )
    {
      for
      (
        size_t i = seastar::this_shard_id();
        i < pkvs_segments_count;
        i += seastar::smp::count
      )
      {
        instances_.push_back( co_await pkvs_t::make( i, memtable_memory_footprint_eviction_threshold ) );
      }
    }

    seastar::future<> stop()
    {
      return seastar::make_ready_future<>();
    }

    seastar::future<std::optional<std::string>> get_item( std::string_view key )
    {
      co_return co_await instances_[ key_to_index( key ) ].get_item( key );
    }

    void insert_item( std::string_view key, std::string_view value )
    {
      instances_[ key_to_index( key ) ].insert_item( key, value );
    }

    void delete_item( std::string_view key )
    {
      instances_[ key_to_index( key ) ].delete_item( key );
    }

    seastar::future<std::set<std::string>> sorted_keys()
    {
      std::set<std::string> keys;

      co_await seastar::coroutine::parallel_for_each(
        instances_,
        [ &keys ]( pkvs_t& pkvs ) -> seastar::future<>
        {
          keys.merge( co_await pkvs.sorted_keys() );
        });

      co_return keys;
    }

    seastar::future<> housekeeping()
    {
      return
        seastar::parallel_for_each(
          instances_,
          []( pkvs_t& pkvs ) -> seastar::future<>
          {
            return pkvs.housekeeping();
          });
    }

  private:
    size_t key_to_index( std::string_view key ) const
    {
      size_t index = key_to_segment_no( key ) / seastar::smp::count;

      assert( index < instances_.size() );

      return index;
    }

    std::vector< pkvs_t > instances_;
  };
}

#endif // PKVS_SHARD_HPP_INCLUDED