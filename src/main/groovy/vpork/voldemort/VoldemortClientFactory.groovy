package vpork.voldemort

import voldemort.client.StoreClientFactory
import voldemort.client.SocketStoreClientFactory
import voldemort.client.ClientConfig

import vpork.HashClient
import vpork.HashClientFactory
import vpork.StatsLogger
import vpork.NodesUtil
import vpork.SetupException


class VoldemortClientFactory implements HashClientFactory {
    private StoreClientFactory storeFact
    private List<String> factoryArgs
    private StatsLogger log

    HashClient createClient() {
        new VoldemortAdapter(storeFact.getStoreClient("bytez"))
    }

    private void snapshotConfigFiles(File srcDir) {
        log.logAndPrint "Copying $srcDir to ${log.logDir}"
        new AntBuilder().copy(toDir: log.logDir) {
            fileset(dir: srcDir)
        }
    }

    void setup(ConfigObject cfg, StatsLogger log, List<String>factoryArgs) {
        if (factoryArgs.size() != 1) {
            throw new SetupException("You must specify a nodes file")
        }

        this.log = log

        snapshotConfigFiles((factoryArgs[0] as File).parentFile)

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
