#include <csignal>
#include <iostream>
#include <unistd.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <string>

namespace tl = thallium;

#define TIMEOUT_MSEC (3000)

class ringProvider : public tl::provider<ringProvider> {
  private:
    std::string prev,next;
    tl::remote_procedure m_join,m_set_prev,m_set_next;
    void join(const tl::request& req,std::string p) {
      std::cout << "prev: " << prev << std::endl;
      tl::endpoint prevServer = get_engine().lookup(prev);
      tl::provider_handle ph(prevServer, 1);
      m_set_next.on(ph)(p);
      std::string oldPrev = prev;
      prev = p;
      std::cout << get_engine().self() << " prev is " << prev << std::endl;
      req.respond(oldPrev);
    }
    void set_next(std::string addr) {
      next=addr;
      std::cout << get_engine().self() << " next is " << addr << std::endl;
    }
    void set_prev(std::string addr) {
      prev=addr;
      std::cout << get_engine().self() << " prev is " << addr << std::endl;
    }
  public:
    ringProvider(tl::engine e,uint16_t provider_id=1)
    : tl::provider<ringProvider>(e,provider_id),
      m_join(define("join",&ringProvider::join)),
      m_set_next(define("set_next",&ringProvider::set_next,tl::ignore_return_value())),
      m_set_prev(define("set_prev",&ringProvider::set_prev,tl::ignore_return_value()))
    {
      get_engine().push_finalize_callback(this,[p=this]() {delete p;});
      prev = next = e.self();
    }
    ~ringProvider() {
      get_engine().pop_finalize_callback();
    }
    
    void call_join(std::string target) {
      tl::endpoint targetServer = get_engine().lookup(target);
      std::cout << targetServer.get_addr() << std::endl;
      tl::provider_handle ph(targetServer, 1);
      std::string self = get_engine().self();
      next = target;
      std::cout << get_engine().self() << " next is " << next << std::endl;

      std::string p = m_join.on(ph)(self);
      std::cout << get_engine().self() << " prev is " << p << std::endl;
      prev = p;
    }
    void call_leave() {
      tl::endpoint prevServer = get_engine().lookup(prev);
      tl::endpoint nextServer = get_engine().lookup(next);
      tl::provider_handle php(prevServer, 1);
      tl::provider_handle phn(nextServer, 1);
      m_set_next.on(php)(next);
      m_set_prev.on(phn)(prev);
    }
};

namespace {
std::function<void(int)> shutdown_handler;
void signal_handler(int signal) { shutdown_handler(signal); }
}

int main(int argc, char *argv[]) {
  tl::engine myEngine("tcp",THALLIUM_SERVER_MODE,true);
  std::cout << "Server running at address " << myEngine.self() << std::endl;
  ringProvider provider(myEngine);
  if(argc > 1) {
    provider.call_join(argv[1]);
  }
  signal(SIGINT,signal_handler);
  shutdown_handler = [&](int signal) {
    provider.call_leave();
    exit(signal);
  };
  myEngine.wait_for_finalize();
  return 0;
}
