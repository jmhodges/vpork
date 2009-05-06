/**
 * 
 */
package vpork.cassandra


import org.apache.cassandra.service.Cassandra;

/**
 * Adapts the Cassandra interface to the one used by VPork
 */
public class CassandraAdapter {
		
	private Cassandra.Client client;
	private String tableName;
	private String columnFamilyColumn;
	
	public CassandraAdapter(Cassandra.Client client, String tableName, String columnFamilyColumn) {
	    this.client = client;
	    this.tableName = tableName;
	    this.columnFamilyColumn = columnFamilyColumn;
	}
	
	def get(String key) {
	    return client.get_column(tableName, key, columnFamilyColumn);
	}
	
	void put(String key, byte[] value) {
	    client.insert(tableName, key, columnFamilyColumn, value, 0, false);
	}
	
}
