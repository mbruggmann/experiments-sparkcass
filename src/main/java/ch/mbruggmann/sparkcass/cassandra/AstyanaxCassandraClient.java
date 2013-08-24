package ch.mbruggmann.sparkcass.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxCassandraClient {

  private final CassandraConfig config;
  private final Keyspace keyspace;
  private final ColumnFamily<String, String> columnFamily;

  public AstyanaxCassandraClient(CassandraConfig config) {
    this.config = config;
    this.keyspace = connect(config);
    this.columnFamily = new ColumnFamily<String, String>(config.getColumnFamily(),
        StringSerializer.get(),
        StringSerializer.get());
  }

  /**
   * Get a column value from cassandra.
   * @param key the row key
   * @param column the column name
   * @return the value for the column as a string, or {@code null} if there is no such column.
   * @throws CassandraClientException if no value can be read from cassandra.
   */
  public String getString(final String key, final String column) throws CassandraClientException {
    try {
      OperationResult<ColumnList<String>> result = keyspace.prepareQuery(columnFamily).getKey(key).execute();
      Column<String> value = result.getResult().getColumnByName(column);
      return (value != null && value.hasValue()) ? value.getStringValue() : null;
    } catch (NotFoundException e) {
      return null;
    } catch (ConnectionException e) {
      throw new CassandraClientException("cant read value for key " + key + ", column " + column, e);
    }
  }

  /**
   * Set a column value in cassandra. Rows and columns will be created automatically if they don't exist yet.
   * @param key the row key
   * @param column the column name
   * @param value the value to set
   * @throws CassandraClientException if the value can't be set in cassandra.
   */
  public void setString(final String key, final String column, final String value) throws CassandraClientException {
    MutationBatch m = keyspace.prepareMutationBatch();
    m.withRow(columnFamily, key).putColumn(column, value, null);
    try {
      m.execute();
    } catch (ConnectionException e) {
      throw new CassandraClientException("cant set value for key " + key + ", column " + column, e);
    }
  }

  private final Keyspace connect(final CassandraConfig config) {
    AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster(config.getCluster())
        .forKeyspace(config.getKeyspace())
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
            .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
            .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM)
            .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM)
        )
        .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("CassandraConnectionPool")
            .setPort(config.getPort())
            .setMaxConnsPerHost(1)
            .setSeeds(config.getSeeds())
            .setConnectionLimiterMaxPendingCount(600)
            .setConnectionLimiterWindowSize(1000)
        )
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
        .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();
    return context.getClient();
  }

}
