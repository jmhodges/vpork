package vpork.hbase

import vpork.HashClient
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes

/**
 * Adapts the Hbase interface to the one used by VPork
 * Instantiates an HTable instance.
 */
public class HbaseGroovyAdapter implements HashClient {
    private HTable table
    private byte[] family
    private byte[] qualifier
    
    public HbaseAdapter(client, String tableName, String columnFamilyColumn) {
        // Create table instance
        this.table = new HTable(Bytes.toBytes(tableName))
        // Cut up the passed in column name into family and qualifier components
        byte [][] pieces = KeyValue.parseColumn(columnFamilyColumn.getBytes())
        this.family = pieces[0]
        this.qualifier = pieces[1]
    }
    
    byte[] get(String key) {
        try {
            // Bytes.toBytes converts 'key' String to byte []
            Get g = new Get(Bytes.toBytes(key))
            g.addColumn(this.family, this.qualifier)
            // Returns first column's value.
            return client.get(g).getValue()
        } catch (NotFound e) {
            return null
        }
    }

    void put(String key, byte[] value) {
        Put p = new Put(Bytes.toByte(key))
        p.add(this.family, this.qualifier, value)
        this.table.put(p)
    }
}
