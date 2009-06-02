package vpork.voldemort


import vpork.StatsLogger

import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import vpork.HashClient

public class Voldemort {
   
    private def cfg
    private List<String> nodes
    private StoreClientFactory storeFact
    private StatsLogger logger
    
    Voldemort(cfg, List<String> nodes, StatsLogger logger) {
        this.cfg           = cfg
        this.nodes         = nodes
        this.logger = logger;
    }
          
    HashClient createClient() {
        new VoldemortAdapter(storeFact.getStoreClient("bytez"))
    }
    
    void setup() {
        String[] bootstrap = generateBootstrapUrls(nodes, cfg.storePort ?: 6666)
        storeFact = new SocketStoreClientFactory(cfg.storeFactory.coreThreads,
                                               cfg.storeFactory.maxThreads,
                                               cfg.storeFactory.maxQueuedRequests,
                                               cfg.storeFactory.maxConnsPerNode,
                                               cfg.storeFactory.maxTotalConns,
                                               bootstrap)
    }
    
    private String[] generateBootstrapUrls(List<String> hosts, int serverPort) {
        hosts.collect { "tcp://${it}:${serverPort}"} as String[]
    }
     
}
