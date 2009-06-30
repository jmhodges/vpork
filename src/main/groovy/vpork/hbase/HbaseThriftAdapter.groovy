package vpork.hbase

import org.apache.hadoop.hbase.thrift.generated.Hbase
import org.apache.hadoop.hbase.thrift.generated.NotFound
import org.apache.hadoop.hbase.thrift.generated.Mutation
import vpork.HashClient

/**
 * Adapts the Hbase interface to the one used by VPork
 * Goes via HBase Thrift interface.
 */

public class HbaseThriftAdapter implements HashClient {
    private Hbase.Client client
    private byte[] tableName
    private byte[] columnFamilyColumn
    
    public HbaseThriftAdapter(Hbase.Client client, String tableName, String columnFamilyColumn) {
        this.client = client
        this.tableName = tableName.getBytes()
        this.columnFamilyColumn = columnFamilyColumn.getBytes()
    }
    
    byte[] get(String key) {
        try {
            return client.get(tableName, key.getBytes(), columnFamilyColumn).value
        } catch (NotFound e) {
            return null
        }
    }

    void put(String key, byte[] value) {
        Mutation m = new Mutation(false, columnFamilyColumn, value)
        ArrayList<Mutation> l = new ArrayList<Mutation>()
        l.add(m)
        client.mutateRow(tableName, key.getBytes(), l)
    }
}
