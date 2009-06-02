package vpork.cassandra


import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import org.apache.cassandra.service.Cassandra
import vpork.HashClient
import vpork.HashClientFactory
import vpork.StatsLogger
import vpork.NodesUtil;

/**
 * Handles setting up a client connection to Cassandra
 */
public class CassandraClientFactory implements HashClientFactory {
   
    private ConfigObject cfg
    private List<String> nodes
    
    private Random r = new Random();
    
    void setup(ConfigObject cfg, StatsLogger logger, List<String>factoryArgs) {
        this.cfg = cfg
        this.nodes = NodesUtil.loadNodes(logger, factoryArgs)
    }

    HashClient createClient() {
        String node = nodes[r.nextInt(nodes.size())]
        TTransport transport = new TSocket(node, cfg.storeFactory.storePort)
        TProtocol protocol = new TBinaryProtocol(transport)
        Cassandra.Client client = new Cassandra.Client(protocol)

        transport.open()
        
        return new CassandraAdapter(client, cfg.storeFactory.tableName, cfg.storeFactory.columnFamilyColumn)
    }
}
