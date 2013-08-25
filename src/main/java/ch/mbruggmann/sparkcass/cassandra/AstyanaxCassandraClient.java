package ch.mbruggmann.sparkcass.cassandra;

import ch.mbruggmann.sparkcass.Config;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AstyanaxCassandraClient {

  private final Keyspace keyspace;
  private final Map<String, ColumnFamily<String, String>> columnFamilies;

  public AstyanaxCassandraClient(Config config) {
    checkNotNull(config);

    this.keyspace = connect(config.getCassandraConfig());
    this.columnFamilies = initializeColumnFamilies(config);
  }

  /**
   * Get a column value from cassandra.
   *
   * @param columnFamily the column family
   * @param key          the row key
   * @param column       the column name
   * @return the value for the column as a string, or {@code null} if there is no such column.
   * @throws IllegalArgumentException if the column family is unknown.
   * @throws CassandraClientException if no value can be read from cassandra.
   */
  public String getString(
      final String columnFamily, final String key, final String column) throws CassandraClientException {
    checkArgument(isKnownColumnFamily(columnFamily), "unknown column family {}", columnFamily);

    final ColumnFamily<String, String> cf = columnFamilies.get(columnFamily);
    try {
      OperationResult<ColumnList<String>> result = keyspace.prepareQuery(cf).getKey(key).execute();
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
   *
   * @param columnFamily the column family
   * @param key          the row key
   * @param column       the column name
   * @param value        the value to set
   * @throws IllegalArgumentException if the column family is unknown.
   * @throws CassandraClientException if the value can't be set in cassandra.
   */
  public void setString(
      final String columnFamily, final String key, final String column, final String value)
      throws CassandraClientException {
    checkArgument(isKnownColumnFamily(columnFamily), "unknown column family {}", columnFamily);
    final ColumnFamily<String, String> cf = columnFamilies.get(columnFamily);

    MutationBatch m = keyspace.prepareMutationBatch();
    m.withRow(cf, key).putColumn(column, value, null);
    try {
      m.execute();
    } catch (ConnectionException e) {
      throw new CassandraClientException("cant set value for key " + key + ", column " + column, e);
    }
  }

  /**
   * Check if a column family is known and accessible by this cassandra client.
   *
   * @param columnFamily the name of the column family.
   * @return true if this is a known column family, false otherwise.
   */
  public boolean isKnownColumnFamily(String columnFamily) {
    return this.columnFamilies.containsKey(columnFamily);
  }

  private final Keyspace connect(final Config.CassandraConfig config) {
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

  private final Map<String, ColumnFamily<String, String>> initializeColumnFamilies(final Config config) {
    checkState(this.keyspace != null, "needs keyspace initialized beforehand");

    // read out the existing column families from the keyspace
    Set<String> existingCFNames = Sets.newHashSet();
    List<ColumnFamilyDefinition> existingCFs = null;
    try {
      existingCFs = this.keyspace.describeKeyspace().getColumnFamilyList();
    } catch (ConnectionException e) {
      throw new IllegalStateException("cant describe keyspace", e);
    }
    for (ColumnFamilyDefinition existingCF: existingCFs) {
      existingCFNames.add(existingCF.getName());
    }

    // create column families according to the configuration, checking they actually exist
    Map<String, ColumnFamily<String, String>> columnFamilies = Maps.newHashMap();
    for (Map.Entry<String, Config.CFConfig> cf : config.getColumnFamilies().entrySet()) {
      final String cfName = cf.getKey();
      if (! existingCFNames.contains(cfName)) {
        throw new IllegalArgumentException("column family doesnt exist: " + cfName);
      }
      columnFamilies.put(cfName, new ColumnFamily<String, String>(
          cfName, StringSerializer.get(), StringSerializer.get()));
    }

    return columnFamilies;
  }

}
