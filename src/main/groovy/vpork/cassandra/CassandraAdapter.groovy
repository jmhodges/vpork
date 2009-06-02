package vpork.cassandra

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.NotFoundException
import vpork.HashClient;

/**
 * Adapts the Cassandra interface to the one used by VPork
 */
public class CassandraAdapter implements HashClient {
	private Cassandra.Client client
	private String tableName
	private String columnFamilyColumn
	
	public CassandraAdapter(Cassandra.Client client, String tableName, String columnFamilyColumn) {
	    this.client = client
	    this.tableName = tableName
	    this.columnFamilyColumn = columnFamilyColumn
	}
	
	byte[] get(String key) {
	    try {
	        return client.get_column(tableName, key, columnFamilyColumn).value
	    } catch (NotFoundException e) {
	        return null
	    }
	}
	
	void put(String key, byte[] value) {
	    client.insert(tableName, key, columnFamilyColumn, value, System.currentTimeMillis(), true)
	}
}
