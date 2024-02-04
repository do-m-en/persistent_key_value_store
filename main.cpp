#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh> // seastar::condition_variable
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/routes.hh>

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

  seastar::future<> service_loop( uint16_t port )
  {
    stop_signal stop_signal;

    seastar::httpd::http_server_control http_server;

    std::cout << "server start\n";
    co_await http_server.start( "pkvs" );

    std::cout << "setting routes\n";
    co_await
      http_server
        .set_routes(
          []( seastar::httpd::routes& r )
          {
            r.add(
              seastar::httpd::operation_type::GET,
              seastar::httpd::url("/get"),
              new seastar::httpd::function_handler(
                []( seastar::httpd::const_req req )
                {
                  std::cout << req.content << std::endl; // FIXME server-side deprecated: use content_stream instead

                  return "get";
                }));

            r.add(
              seastar::httpd::operation_type::POST,
              seastar::httpd::url("/post"),
              new seastar::httpd::function_handler(
                []( seastar::httpd::const_req req )
                {
                  std::cout << req.content << std::endl; // FIXME server-side deprecated: use content_stream instead

                  return "post";
                }));

            r.add(
              seastar::httpd::operation_type::POST,
              seastar::httpd::url("/delete"),
              new seastar::httpd::function_handler(
                []( seastar::httpd::const_req req )
                {
                  std::cout << req.content << std::endl; // FIXME server-side deprecated: use content_stream instead

                  return "delete";
                }));
          });

    std::cout << "listening on port " << port << '\n';
    co_await http_server.listen(seastar::ipv4_addr("0.0.0.0", port));

    co_await stop_signal.wait();

    std::cout << "shutting down\n";
    co_await http_server.stop(); // TODO RAII
  }
}

int main( int argc, char** argv )
{
  seastar::app_template app;

  app.add_options()(
    "port,p",
    boost::program_options::value<uint16_t>()->default_value( 8080 ),
    "HTTP Server port");

  try
  {
    app.run(
      argc,
      argv,
      [ &app ]
      {
        auto&& configuration = app.configuration();
        return service_loop( configuration["port"].as<uint16_t>() );
      });
  }
  catch (...)
  {
      std::cerr << "Failed to start: " << std::current_exception() << '\n';

      return 1;
  }

  return 0;
}