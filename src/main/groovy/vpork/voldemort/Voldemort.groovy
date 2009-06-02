package vpork.voldemort

import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import voldemort.client.ClientConfig

import vpork.HashClient
import vpork.StatsLogger


public class Voldemort {
    private def cfg
    private List<String> nodes
    private StoreClientFactory storeFact
    private StatsLogger logger


    Voldemort(cfg, List<String> nodes, StatsLogger logger) {
        this.cfg    = cfg
        this.nodes  = nodes
        this.logger = logger;
    }
          
    HashClient createClient() {
        new VoldemortAdapter(storeFact.getStoreClient("bytez"))
    }
    
    void setup() {
        String[] bootstrap = generateBootstrapUrls(nodes, cfg.storePort ?: 6666)
        ClientConfig voldConfig = new ClientConfig()

        voldConfig.bootstrapUrls = bootstrap
        voldConfig.maxThreads = cfg.storeFactory.maxThreads
        voldConfig.maxConnectionsPerNode = cfg.storeFactory.maxConnsPerNode

        if (cfg.storeFactory.maxQueuedRequests) {
            voldConfig.maxQueuedRequests = cfg.storeFactory.maxQueuedRequests
        }
        if (cfg.storeFactory.maxTotalConns) {
            voldConfig.maxTotalConnections = cfg.storeFactory.maxTotalConns
        }

        // TODO:  Fix JMX classloader issues with Groovy
        voldConfig.enableJmx = false
        storeFact = new SocketStoreClientFactory(voldConfig)
    }
    
    private String[] generateBootstrapUrls(List<String> hosts, int serverPort) {
        hosts.collect { "tcp://${it}:${serverPort}"} as String[]
    }
}
