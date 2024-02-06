#ifndef PKVS_HPP_INCLUDED
#define PKVS_HPP_INCLUDED

#include <seastar/core/future.hh>

namespace pkvs
{
  class pkvs_t
  {
  public:
    pkvs_t( size_t instance_no );

    seastar::future<std::optional<std::string>> get_item( std::string_view key ) const;
    // return true if new item, false on update of existing item
    seastar::future<bool> insert_item( std::string_view key, std::string_view value );
    seastar::future<bool> delete_item( std::string_view key );

  private:
    size_t instance_no_;
  };
}

#endif // PKVS_HPP_INCLUDED