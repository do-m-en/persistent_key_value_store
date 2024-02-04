#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <iostream>

/*seastar::future<> reply()
{

}*/

const char* canned_response = "Seastar is the future!\n";

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

              std::cout << "Accepted connection from " << res.remote_address << "\n";

              auto out = res.connection.output();

              co_await out.write(canned_response);
              co_await out.close();
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
            [] (unsigned c)
            {
              return
                seastar::smp::submit_to(
                  c,
                  /*[]() -> seastar::future<>
                  {
                    std::cout << "Hello world\n";
                    std::cout << seastar::smp::count << "\n";
                    co_return;
                  }*/
                  service_loop);
            });
      });
}