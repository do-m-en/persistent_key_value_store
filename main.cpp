#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <iostream>

seastar::future<> handle_connection
(
  seastar::connected_socket& socket,
  seastar::socket_address const& address
)
{
  const char* canned_response = "Seastar is the future!\n";

  std::cout << "Accepted connection from " << address << "\n";

  auto out = socket.output();

  co_await out.write(canned_response);
  co_await out.close();
}

seastar::future<> service_loop()
{
  return
    seastar::do_with(
      seastar::listen(
        seastar::make_ipv4_address({8080}),
        []
        {
          // support fast server restarts by allowing reuse the local address port
          // otherwise server needs to wait for the connections that were already
          // closed but need to waith for the TIME_WAIT duration before freeing the port
          seastar::listen_options lo;
          lo.reuse_address = true;

          return lo;
        }()),
      []( auto& listener )
      {
        return
          seastar::keep_doing(
            [&listener]() -> seastar::future<>
            {
              auto res = co_await listener.accept();
              try
              {
                co_await handle_connection( res.connection, res.remote_address );
              }
              catch( ... )
              {
                fmt::print(
                  stderr,
                  "Could not handle connection: {}\n",
                  std::current_exception());
              }
            });
      });
}

int main( int argc, char** argv )
{
    seastar::app_template app;
    app.run(
      argc,
      argv,
      [] -> seastar::future<>
      {
        return
          seastar::parallel_for_each(boost::irange<unsigned>(0, seastar::smp::count),
            [](unsigned c)
            {
              return seastar::smp::submit_to( c, service_loop );
            });
      });
}