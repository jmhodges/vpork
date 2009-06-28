package vpork.hbase

import org.apache.hadoop.hbase.thrift.generated.Hbase
import org.apache.hadoop.hbase.thrift.generated.NotFound
import org.apache.hadoop.hbase.thrift.generated.Mutation
import vpork.HashClient

/**
 * Adapts the Hbase interface to the one used by VPork
 */

public class HbaseAdapter implements HashClient {
    private Hbase.Client client
    private byte[] tableName
    private byte[] columnFamilyColumn
    
    public HbaseAdapter(Hbase.Client client, String tableName, String columnFamilyColumn) {
        this.client = client
        this.tableName = tableName.getBytes()
        this.columnFamilyColumn = columnFamilyColumn.getBytes()
    }
    
    byte[] get(String key) {
        try {
            return table.get(tableName, key.getBytes(), columnFamilyColumn).value
        } catch (NotFound e) {
            return null
        }
    }

    void put(String key, byte[] value) {
        Mutation m = new Mutation(false, columnFamilyColumn, value)
        client.mutateRow(tableName, key.toBytes(), m)
    }
}
