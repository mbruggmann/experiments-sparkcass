package ch.mbruggmann.sparkcass.cassandra;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class CassandraClientException extends Exception {

  public CassandraClientException(String msg, ConnectionException e) {
    super(msg, e);
  }

}
