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
#include <chrono>

namespace tl = thallium;

std::string get_coordinator(std::vector<std::string> &addrs) {
  return *std::max_element(addrs.begin(),addrs.end(),[](std::string a,std::string b) {
    return strcmp(a.c_str(),b.c_str())>0;
  });
}

const int DURATION = 2;
const int TIMEOUT = 5;

class ringProvider : public tl::provider<ringProvider> {
  private:
    std::string self,prev,next;
    tl::remote_procedure m_join,m_set_prev,m_set_next,m_list,m_election,m_coordinator;
    std::string coord;
    std::chrono::system_clock::time_point last_notify;
    tl::mutex mu;

    void join(const tl::request& req,std::string p) {
      if(self==get_prev()) {
        assert(self==get_next());
        set_prev(p);
        set_next(p);
        req.respond(self);
        return;
      }
      tl::endpoint prevServer = get_engine().lookup(get_prev());
      tl::provider_handle ph(prevServer, 1);
      m_set_next.on(ph)(p);
      std::string oldPrev = get_prev();
      set_prev(p);
      req.respond(oldPrev);
    }
    std::string get_next() {
      mu.lock();
      std::string r = next;
      mu.unlock();
      return r;
    }
    void set_next(std::string addr) {
      mu.lock();
      next=addr;
      std::cout << "prev is " << prev << std::endl;
      std::cout << "next is " << next << std::endl;
      mu.unlock();
    }
    std::string get_prev() {
      mu.lock();
      std::string r = prev;
      mu.unlock();
      return r;
    }
    void set_prev(std::string addr) {
      mu.lock();
      prev=addr;
      std::cout << "prev is " << prev << std::endl;
      std::cout << "next is " << next << std::endl;
      mu.unlock();
    }
    std::string get_coord() {
      mu.lock();
      std::string r = coord;
      mu.unlock();
      return r;
    }
    void set_coord(std::string addr) {
      mu.lock();
      coord=addr;
      mu.unlock();
    }
    void list(std::vector<std::string> addrs) {
      assert(addrs.size()>0);
      last_notify = std::chrono::system_clock::now();      
      if(addrs[0]==self) {
        std::cout << "heart beat emitted.. nodes is " << std::endl;
        for(std::string addr:addrs) {
          std::cout << addr << std::endl;
        }
        return;
      }
      std::cout << "heart beat received.. " << std::endl;
      addrs.push_back(self);
      if(get_coord() != "" && addrs[0]!=get_coord()) {
        last_notify = std::chrono::system_clock::now() - std::chrono::seconds(2*TIMEOUT);
      }
      tl::endpoint server = get_engine().lookup(get_next());
      tl::provider_handle ph(server, 1);
      m_list.on(ph)(addrs);
    }
    void election(const tl::request& req,std::string host) {
      if(host==self) {
        std::vector<std::string> x{self};
        req.respond(x);
        return;
      }
      tl::endpoint server = get_engine().lookup(get_next());
      tl::provider_handle ph(server, 1);
      std::vector<std::string> addrs = m_election.on(ph)(host);
      addrs.push_back(self);
      req.respond(addrs);
      return;
    }
    void coordinator(std::vector<std::string> addrs,std::string host) {
      assert(addrs.size()>0);
      std::string oldcoord = get_coord();
      set_coord(get_coordinator(addrs));
      if(oldcoord==get_coord()) {
        return;
      }
      std::cout << "coord is " << get_coord() << std::endl;
      tl::endpoint server = get_engine().lookup(get_next());
      tl::provider_handle ph(server, 1);
      m_coordinator.on(ph)(addrs,self);
      return;
    }
    bool is_coordinator() {
      return self == get_coord();
    }
  public:
    ringProvider(tl::engine e,uint16_t provider_id=1)
    : tl::provider<ringProvider>(e,provider_id),
      m_join(define("join",&ringProvider::join)),
      m_set_next(define("set_next",&ringProvider::set_next)),
      m_set_prev(define("set_prev",&ringProvider::set_prev)),
      m_list(define("list",&ringProvider::list,tl::ignore_return_value())),
      m_election(define("election",&ringProvider::election)),
      m_coordinator(define("coordinator",&ringProvider::coordinator,tl::ignore_return_value()))
    {
      get_engine().push_finalize_callback(this,[p=this]() {delete p;});
      prev = next = self = get_engine().self();
    }
    ~ringProvider() {
      m_join.deregister();
      m_set_next.deregister();
      m_set_prev.deregister();
      m_list.deregister();
      m_election.deregister();
      m_coordinator.deregister();
      get_engine().pop_finalize_callback(this);
    }
    
    void call_join(std::string target) {
      tl::endpoint targetServer = get_engine().lookup(target);
      tl::provider_handle ph(targetServer, 1);
      set_next(target);
      std::string p = m_join.on(ph)(self);
      set_prev(p);
    }
    void leave() {
      if(self==get_prev()) {
        assert(self==get_next());
        return;
      }
      tl::endpoint server = get_engine().lookup(get_prev());
      tl::provider_handle ph(server, 1);
      m_set_next.on(ph)(get_next());
      server = get_engine().lookup(get_next());
      ph = tl::provider_handle(server, 1);
      m_set_prev.on(ph)(get_prev());
    }
    void call_list() {
      std::vector<std::string> addrs(1,self);
      tl::endpoint server = get_engine().lookup(get_next());
      tl::provider_handle ph(server, 1);
      m_list.on(ph)(addrs);
    }
    void tick() {
      if(is_coordinator()) {
        call_list();
        return;
      }
      std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
      int64_t diff = std::chrono::duration_cast<std::chrono::seconds>(now-last_notify).count();
      if(diff < TIMEOUT) {
        return;
      }
      if(get_next()==self) {
        assert(get_prev()==self);
        set_coord(self);
        return;
      }
      std::cout << "timeout, maybe coordinator dead." << std::endl;
      tl::endpoint server = get_engine().lookup(get_next());
      tl::provider_handle ph(server, 1);
      std::vector<std::string> addrs = m_election.on(ph)(self);
      std::cout << "election finished : " << std::endl;
      for(std::string addr:addrs) {
        std::cout << addr << std::endl;
      }
      m_coordinator.on(ph)(addrs,self);
    }
};

int main(int argc, char *argv[]) {
  static sigset_t ss;
  sigemptyset(&ss);
  sigaddset(&ss, SIGINT);
  sigaddset(&ss, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &ss, NULL);

  tl::engine myEngine("tcp",THALLIUM_SERVER_MODE);
  std::cout << "Server running at address " << myEngine.self() << std::endl;

  ringProvider provider(myEngine);

  std::thread sig([&]{
    int num=0;
    sigwait(&ss,&num);
    provider.leave();
    std::cout << "signal received " << num << std::endl;
    exit(1);
  });

  std::thread tick([&]{
    const std::chrono::seconds interval(DURATION);
    while(1) {
      std::this_thread::sleep_for(interval);
      provider.tick();
    }
  });

  if(argc > 1) {
    provider.call_join(argv[1]);
  }

  myEngine.wait_for_finalize();
  return 0;
}
