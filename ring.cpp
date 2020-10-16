#include <csignal>
#include <iostream>
#include <unistd.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <string>
#include <cassert>
#include <thread>
#include <pthread.h>

namespace tl = thallium;

class ringProvider : public tl::provider<ringProvider> {
  private:
    std::string self,prev,next;
    tl::remote_procedure m_join,m_set_prev,m_set_next,m_list;
    void join(const tl::request& req,std::string p) {
      if(self==prev) {
        assert(self==next);
        std::cout << "join: self == prev == next, so prev and next is p, oldPrev is self" << std::endl;
        prev = next = p;
        req.respond(self);
        return;
      }
      tl::endpoint prevServer = get_engine().lookup(prev);
      tl::provider_handle ph(prevServer, 1);
      m_set_next.on(ph)(p);
      std::string oldPrev = prev;
      prev = p;
      req.respond(oldPrev);
    }
    void set_next(std::string addr) {
      next=addr;
      std::cout << "next is " << addr << std::endl;
    }
    void set_prev(std::string addr) {
      prev=addr;
      std::cout << "prev is " << addr << std::endl;
    }
    void list(std::vector<std::string> addrs) {
      assert(addrs.size()>0);
      if(addrs[0]==self) {
        std::cout << "list rpc:" << std::endl;
        for(std::string addr:addrs) {
          std::cout << addr << std::endl;
        }
        return;
      }
      assert(addrs.back()==prev);
      addrs.push_back(self);
      tl::endpoint server = get_engine().lookup(next);
      tl::provider_handle ph(server, 1);
      m_list.on(ph)(addrs);
      return;
    }
  public:
    ringProvider(tl::engine e,uint16_t provider_id=1)
    : tl::provider<ringProvider>(e,provider_id),
      m_join(define("join",&ringProvider::join)),
      m_set_next(define("set_next",&ringProvider::set_next)),
      m_set_prev(define("set_prev",&ringProvider::set_prev)),
      m_list(define("m_list",&ringProvider::list,tl::ignore_return_value()))
    {
      get_engine().push_finalize_callback(this,[p=this]() {delete p;});
      prev = next = self = get_engine().self();
    }
    ~ringProvider() {
      m_join.deregister();
      m_set_next.deregister();
      m_set_prev.deregister();
      m_list.deregister();
      get_engine().pop_finalize_callback(this);
    }
    
    void call_join(std::string target) {
      tl::endpoint targetServer = get_engine().lookup(target);
      tl::provider_handle ph(targetServer, 1);
      next = target;
      std::string p = m_join.on(ph)(self);
      prev = p;
    }
    void call_leave() {
      if(self==prev) {
        assert(self==next);
        std::cout << "self == prev == next, so exit without no action" << std::endl;
        return;
      }
      tl::endpoint server = get_engine().lookup(prev);
      tl::provider_handle ph(server, 1);
      m_set_next.on(ph)(next);
      server = get_engine().lookup(next);
      ph = tl::provider_handle(server, 1);
      m_set_prev.on(ph)(prev);
    }
    void call_list() {
      std::vector<std::string> addrs(1,self);
      tl::endpoint server = get_engine().lookup(next);
      tl::provider_handle ph(server, 1);
      m_list.on(ph)(addrs);
    }
};

int main(int argc, char *argv[]) {
  pthread_t t;
  static sigset_t ss;
  sigemptyset(&ss);
  sigaddset(&ss, SIGINT);
  sigaddset(&ss, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &ss, NULL);

  tl::engine myEngine("tcp",THALLIUM_SERVER_MODE);
  std::cout << "Server running at address " << myEngine.self() << std::endl;

  ringProvider provider(myEngine);

  std::thread th([&]{
    std::cout << "wait SIGINT" << std::endl;
    int num=0;
    sigwait(&ss,&num);
    provider.call_leave();
    exit(1);
  });

  if(argc > 1) {
    std::cout << "call_join" << std::endl;
    provider.call_join(argv[1]);
  }
  std::cout << "call_list" << std::endl;
  provider.call_list();
  std::cout << "wait_for_finalize" << std::endl;
  myEngine.wait_for_finalize();
  return 0;
}
