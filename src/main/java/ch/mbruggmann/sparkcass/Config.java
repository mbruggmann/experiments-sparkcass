package ch.mbruggmann.sparkcass;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Sparkcass configuration.
 * Backed by a json config file.
 */
public class Config {
  private final CassandraConfig cassandraConfig;
  private final ServiceConfig serviceConfig;
  private final Map<String, CFConfig> columnFamilies;

  public Config(
      @JsonProperty("cassandra") CassandraConfig cassandraConfig,
      @JsonProperty("service") ServiceConfig serviceConfig,
      @JsonProperty("columnfamilies") Map<String, CFConfig> columnFamilies) {
    this.cassandraConfig = cassandraConfig;
    this.serviceConfig = serviceConfig;
    this.columnFamilies = columnFamilies;
  }

  public CassandraConfig getCassandraConfig() {
    return this.cassandraConfig;
  }

  public ServiceConfig getServiceConfig() {
    return this.serviceConfig;
  }

  public Map<String, CFConfig> getColumnFamilies() {
    return this.columnFamilies;
  }

  public static class CassandraConfig {
    private final String seeds;
    private final int port;
    private final String cluster;
    private final String keyspace;

    public CassandraConfig(
        @JsonProperty("seeds") String seeds,
        @JsonProperty("port") int port,
        @JsonProperty("cluster") String cluster,
        @JsonProperty("keyspace") String keyspace) {
      this.seeds = seeds;
      this.port = port;
      this.cluster = cluster;
      this.keyspace = keyspace;
    }

    public String getSeeds() {
      return this.seeds;
    }

    public int getPort() {
      return this.port;
    }

    public String getCluster() {
      return this.cluster;
    }

    public String getKeyspace() {
      return this.keyspace;
    }

  }

  public static class ServiceConfig {
    private final int port;

    public ServiceConfig(
        @JsonProperty("port") int port) {
      this.port = port;
    }

    public int getPort() {
      return this.port;
    }
  }

  public static class CFConfig {
    private final String owner;
    private final String access;
    private final int ratelimit;

    public CFConfig(
        @JsonProperty("owner") String owner,
        @JsonProperty("access") String access,
        @JsonProperty("rate-limit") int ratelimit) {
      this.owner = owner;
      this.access = access;
      this.ratelimit = ratelimit;
    }

    public String getOwner() {
      return this.owner;
    }

    public String getAccess() {
      return this.access;
    }

    public int getRatelimit() {
      return this.ratelimit;
    }
  }

  public static Config fromFile(File file) {
    final ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(file, Config.class);
    } catch (IOException e) {
      throw new RuntimeException("can't read config file", e);
    }
  }


}
