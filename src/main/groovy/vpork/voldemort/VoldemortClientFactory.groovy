package vpork.voldemort

import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import voldemort.client.ClientConfig

import vpork.HashClient
import vpork.HashClientFactory
import vpork.StatsLogger
import vpork.SetupException
import vpork.NodesUtil


class VoldemortClientFactory implements HashClientFactory {
    private StoreClientFactory storeFact
    private List<String> factoryArgs
    private StatsLogger log

    HashClient createClient() {
        new VoldemortAdapter(storeFact.getStoreClient("bytez"))
    }

    void setup(ConfigObject cfg, StatsLogger log, List<String>factoryArgs) {
        this.log = log

        List<String> nodes = NodesUtil.loadNodes(log, factoryArgs)
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
