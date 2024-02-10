//  Copyright 2024 Domen Vrankar
//
//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh> // seastar::condition_variable
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/routes.hh>

#include <nlohmann/json.hpp>

#include <expected>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>

#include "pkvs/pkvs_shard.hpp"

#include <iostream>

namespace
{
  class stop_signal
  {
  public:
    stop_signal()
    {
      seastar::engine().handle_signal(
        SIGINT,
        [this] { signaled(); });
      seastar::engine().handle_signal(
        SIGTERM,
        [this] { signaled(); });
    }

    ~stop_signal()
    {
      seastar::engine().handle_signal(SIGINT, [] {});
      seastar::engine().handle_signal(SIGTERM, [] {});
    }

    seastar::future<> wait() { return cv_.wait([this] { return caught_; }); }
    bool stopping() const { return caught_; }

  private:
    void signaled()
    {
      if( caught_ )
        return;

      caught_ = true;
      cv_.broadcast();
    }

    bool caught_ = false;
    seastar::condition_variable cv_;
  };

  seastar::future<> service_loop
  (
    uint16_t port,
    size_t memtable_memory_footprint_eviction_threshold
  )
  {
    stop_signal signal;
    seastar::sharded< pkvs::pkvs_shard > store;

    std::cout << "running on: " << seastar::smp::count << '\n';

    co_await store.start();
    co_await store.invoke_on_all(
      [ memtable_memory_footprint_eviction_threshold ]( pkvs::pkvs_shard& local_shard )
      {
        return local_shard.run( memtable_memory_footprint_eviction_threshold );
      });

    seastar::httpd::http_server_control http_server;

    std::cout << "server start\n";
    co_await http_server.start( "pkvs" );

    std::cout << "setting routes\n";
    co_await
      http_server
        .set_routes(
          [ &store ]( seastar::httpd::routes& r )
          {
            auto common_request_processing =
              []
              (
                seastar::http::request& req,
                std::unordered_set< std::string > expected_keys
              )
                ->
                  std::expected
                  <
                    std::tuple
                    <
                      nlohmann::json,
                      size_t
                    >,
                    std::string
                  >
              {
                  // FIXME server-side deprecated: use content_stream instead
                  // throws if data is not in utf-8 format
                  try
                  {
                    nlohmann::json data = nlohmann::json::parse( req.content.c_str() );

                    if( expected_keys.contains( "key" ) == false )
                      return std::unexpected("{\"result\":\"internal server error\"}");

                    for( auto& [key, val] : data.items() )
                    {
                      if
                      (
                        expected_keys.erase( key ) == 0 ||
                        val.type() != nlohmann::json::value_t::string
                      )
                      {
                        return std::unexpected("{\"result\":\"request error\"}");
                      }
                    }

                    if( expected_keys.empty() == false )
                      return std::unexpected("{\"result\":\"request error\"}");

                    auto key = data["key"].template get<std::string_view>();

                    if( key.empty() || key.size() > 256 )
                      return std::unexpected("{\"result\":\"invalid key size\"}");

                    size_t shard_no = pkvs::key_to_shard_no( key );

                    return std::tuple{ std::move( data ), shard_no };
                  }
                  catch( ... )
                  {
                    return std::unexpected("{\"result\":\"request error\"}");
                  }
              };
            r.add(
              seastar::httpd::operation_type::GET,
              seastar::httpd::url("/get"),
              new seastar::httpd::function_handler(
                [ &store, common_request_processing ]
                (
                  std::unique_ptr<seastar::http::request> req
                ) -> seastar::future<seastar::json::json_return_type>
                {
                  auto processed = common_request_processing( *req, { "key" } );

                  if( processed.has_value() == false )
                    co_return processed.error();

                  auto const& [ data, shard_no ] = *processed;

                  auto result =
                    co_await
                      store.invoke_on(
                        shard_no,
                        [key = data["key"].template get<std::string_view>()]
                        (
                          pkvs::pkvs_shard& local_shard
                        )
                        {
                          return local_shard.get_item( key );
                        });

                  if( result != std::nullopt )
                    co_return "{\"value\":\"" + result.value() + "\"}";

                  co_return "{\"result\":\"missing\"}";
                }));

            r.add(
              seastar::httpd::operation_type::POST,
              seastar::httpd::url("/post"),
              new seastar::httpd::function_handler(
                [ &store, common_request_processing ]
                (
                  std::unique_ptr<seastar::http::request> req
                ) -> seastar::future<seastar::json::json_return_type>
                {
                  auto processed = common_request_processing( *req, { "key", "value" } );

                  if( processed.has_value() == false )
                    co_return processed.error();

                  auto const& [ data, shard_no ] = *processed;

                  co_await
                    store.invoke_on(
                      shard_no,
                      [
                        key = data["key"].template get<std::string_view>(),
                        value = data["value"].template get<std::string_view>()
                      ]
                      (
                        pkvs::pkvs_shard& local_shard
                      )
                      {
                        return local_shard.insert_item( key, value );
                      });

                  co_return "{\"result\":\"ok\"}";
                }));

            r.add(
              seastar::httpd::operation_type::POST,
              seastar::httpd::url("/delete"),
              new seastar::httpd::function_handler(
                [ &store, common_request_processing ]
                (
                  std::unique_ptr<seastar::http::request> req
                ) -> seastar::future<seastar::json::json_return_type>
                {
                  auto processed = common_request_processing( *req, { "key" } );

                  if( processed.has_value() == false )
                    co_return processed.error();

                  auto const& [ data, shard_no ] = *processed;

                  co_await
                    store.invoke_on(
                      shard_no,
                      [key = data["key"].template get<std::string_view>()]
                      (
                        pkvs::pkvs_shard& local_shard
                      )
                      {
                        return local_shard.delete_item( key );
                      });

                  co_return "{\"result\":\"ok\"}";
                }));

            r.add(
              seastar::httpd::operation_type::GET,
              seastar::httpd::url("/sorted_keys"),
              new seastar::httpd::function_handler(
                [ &store, common_request_processing ]
                (
                  std::unique_ptr<seastar::http::request> req
                ) -> seastar::future<seastar::json::json_return_type>
                {
                  std::set< std::string > keys;

                  try
                  {
                    co_await seastar::coroutine::parallel_for_each(
                      std::views::iota( 0u, seastar::smp::count ),
                      [ &store, &keys ]( size_t shard_no ) -> seastar::future<>
                      {
                        keys.merge(
                          co_await
                            store.invoke_on(
                              shard_no,
                              []( pkvs::pkvs_shard& local_shard )
                              {
                                return local_shard.sorted_keys();
                              }));
                      });
                  }
                  catch( ... )
                  {
                    std::cerr << "keys failed: " << std::current_exception() << '\n';

                    co_return "{\"result\":\"internal server error\"}";
                  }

                  std::string result{ "{\"keys\":[" };

                  for( auto const& key : keys )
                    result += '"' + key + "\",";

                  if( keys.empty() == false )
                    result.pop_back();

                  result += "]}";

                  co_return result;
                }));
          });

    std::cout << "try listening on port " << port << '\n';
    co_await http_server.listen(seastar::ipv4_addr("0.0.0.0", port));
    std::cout << "listening\n";

    co_await
      [&] -> seastar::future<>
      {
        while( signal.stopping() == false )
        {
          co_await seastar::sleep( std::chrono::seconds( 1 ) );

          co_await seastar::coroutine::parallel_for_each(
            std::views::iota( 0u, seastar::smp::count ),
            [ &store ]( size_t shard_no ) -> seastar::future<>
            {
              co_await
                store.invoke_on(
                  shard_no,
                  []( pkvs::pkvs_shard& local_shard )
                  {
                    return local_shard.housekeeping();
                  });
            });
        }
      }()
      .finally(
        seastar::coroutine::lambda(
          [&] -> seastar::future<>
          {
            std::cout << "shutting down\n";
            co_await http_server.stop();
            co_await store.stop();
          }));
  }
}

int main( int argc, char** argv )
{
  seastar::app_template app;

  app.add_options()(
    "port,p",
    boost::program_options::value<uint16_t>()->default_value( 8080 ),
    "HTTP Server port");
  app.add_options()(
    "memory_threshold,t",
    boost::program_options::value<size_t>()->default_value( 100000000 ),
    "HTTP Server port");

  try
  {
    app.run(
      argc,
      argv,
      [ &app ]
      {
        auto&& configuration = app.configuration();
        return
          service_loop(
            configuration["port"].as<uint16_t>(),
            configuration["memory_threshold"].as<size_t>() );
      });
  }
  catch (...)
  {
      std::cerr << "Failed to start: " << std::current_exception() << '\n';

      return 1;
  }

  return 0;
}