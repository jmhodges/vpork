package vpork.hbase

import vpork.HashClient
import vpork.HashClientFactory
import vpork.StatsLogger
import vpork.NodesUtil;

/**
 * Handles setting up a client connection to HBase
 * using native HTable called from groovy.
 */
public class HbaseGroovyClientFactory implements HashClientFactory {
    private ConfigObject cfg

    void setup(ConfigObject cfg, StatsLogger logger, List<String> factoryArgs) {
        this.cfg = cfg
    }

    HashClient createClient() {
        // Where hbase connects is configured in zoo.cfg
        // Clients go to the ZooKeeper Quorum designated
        // in zoo.cfg which is read off the CLASSPATH when
        // ever client tries to connect.
        return new HbaseGroovyAdapter(cfg.storeFactory.tableName, cfg.storeFactory.columnFamilyColumn)
    }
}
