#ifndef MEMTABLE_HPP_INCLUDED
#define MEMTABLE_HPP_INCLUDED

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/member.hpp>

#include <chrono>

namespace pkvs
{
  using time_point_t = std::chrono::time_point< std::chrono::steady_clock >;

  enum class entry_type_t
  {
    tombstone,
    value,
    pointer
  };

  struct entry_t
  {
    std::string key;
    std::string content;
    entry_type_t type;
    bool dirty;
    time_point_t last_accessed;

    entry_t( std::string_view in_key, std::string_view in_content, bool in_dirty )
      : key{ in_key }
      , content{ in_content }
      , type{ entry_type_t::value }
      , dirty{ in_dirty }
      , last_accessed{ std::chrono::steady_clock::now() }
    {}

    explicit entry_t( std::string_view in_key )
      : key{ in_key }
      , type{ entry_type_t::tombstone }
      , dirty{ true }
      , last_accessed{ std::chrono::steady_clock::now() }
    {}

    static entry_t make_tombstone( std::string_view key )
    {
      return entry_t{ key };
    }

    // copies are needed because boost multiindex doesn't support move...

    bool operator<( entry_t const& e ) const { return key < e.key; }
  };

  struct key_index;
  struct last_accessed_index;

  using memtable_t =
    boost::multi_index::multi_index_container
    <
      entry_t,
      boost::multi_index::indexed_by
      <
        // sort by entry_t::operator<
        boost::multi_index::ordered_unique
        <
          boost::multi_index::tag< key_index >,
          boost::multi_index::identity< entry_t >
        >,
        // sort by less< time_point_t > on last_accessed
        boost::multi_index::ordered_non_unique
        <
          boost::multi_index::tag< last_accessed_index >,
          boost::multi_index::member
          <
            entry_t,
            time_point_t,
            &entry_t::last_accessed
          >
        >
      >
    >;
}

#endif // MEMTABLE_HPP_INCLUDED