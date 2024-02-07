#ifndef PKVS_HPP_INCLUDED
#define PKVS_HPP_INCLUDED

#include <seastar/core/future.hh>
#include <string_view>
#include "detail/memtable.hpp"

namespace pkvs
{
  class pkvs_t
  {
  public:
    pkvs_t( size_t instance_no );

    // contract: assert( key.empty() == false && key.size() < 256 );
    seastar::future<std::optional<std::string>> get_item( std::string_view key ) const;
    // contract: assert( key.empty() == false && key.size() < 256 );
    seastar::future<> insert_item( std::string_view key, std::string_view value );
    // contract: assert( key.empty() == false && key.size() < 256 );
    seastar::future<> delete_item( std::string_view key );

  private:
    size_t instance_no_;
    memtable_t memtable_;
  };
}

#endif // PKVS_HPP_INCLUDED