//  Copyright 2024 Domen Vrankar
//
//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

#ifndef SSTABLES_HPP_INCLUDED
#define SSTABLES_HPP_INCLUDED

#include <seastar/core/future.hh>
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace pkvs
{
  struct sstable_item_t
  {
    std::string key;
    std::optional<std::string> value;
  };

  class sstables_t
  {
  public:
    static seastar::future<sstables_t> make( std::filesystem::path base_path );

    // contract: assert( key.empty() == false && key.size() < 256 );
    seastar::future<std::optional<std::string>> get_item( std::string_view key );
    seastar::future<std::set<std::string>> sorted_keys();

    seastar::future<> store( std::span< sstable_item_t > items );
    seastar::future<> try_merge_oldest();

  private:
    sstables_t
    (
      std::filesystem::path base_path,
      std::vector<unsigned long>&& sstables
    );

    std::filesystem::path base_path_;
    std::vector<unsigned long> sstables_;
  };
}

#endif // SSTABLES_HPP_INCLUDED