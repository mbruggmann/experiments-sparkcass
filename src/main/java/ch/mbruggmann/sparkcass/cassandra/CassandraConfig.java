package ch.mbruggmann.sparkcass.cassandra;

public class CassandraConfig {

  private final String seeds;
  private final int port;
  private final String clusterName;
  private final String keyspaceName;
  private final String columnFamily;

  public CassandraConfig() {
    this.seeds = "127.0.0.1:9160";
    this.port = 9160;
    this.clusterName = "test";
    this.keyspaceName = "test";
    this.columnFamily = "test";
  }

  public String getSeeds() {
    return this.seeds;
  }

  public int getPort() {
    return this.port;
  }

  public String getCluster() {
    return this.clusterName;
  }

  public String getKeyspace() {
    return this.keyspaceName;
  }

  public String getColumnFamily() {
    return this.columnFamily;
  }

}
