package ch.mbruggmann.sparkcass;

import ch.mbruggmann.sparkcass.cassandra.AstyanaxCassandraClient;
import ch.mbruggmann.sparkcass.cassandra.CassandraClientException;
import ch.mbruggmann.sparkcass.cassandra.CassandraConfig;
import spark.Request;
import spark.Response;
import spark.Route;

import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

public class Service {

  public static void main(String[] args) {

    final CassandraConfig config = new CassandraConfig();
    final AstyanaxCassandraClient client = new AstyanaxCassandraClient(config);

    get(new Route("/:key/:column") {
      @Override
      public Object handle(Request request, Response response) {
        final String key = request.params(":key");
        final String column = request.params(":column");
        if (key.isEmpty() || column.isEmpty()) {
          halt(StatusCode.BAD_REQUEST.getCode());
        }

        String value = null;
        try {
          value = client.getString(key, column);
        } catch (CassandraClientException e) {
          e.printStackTrace();
          halt(StatusCode.SERVICE_UNAVAILABLE.getCode());
        }

        if (value == null) {
          halt(StatusCode.NOT_FOUND.getCode());
        }

        return value;
      }
    });

    post(new Route("/:key/:column") {
      @Override
      public Object handle(Request request, Response response) {
        final String key = request.params(":key");
        final String column = request.params(":column");
        if (key.isEmpty() || column.isEmpty())
          halt(StatusCode.BAD_REQUEST.getCode());

        final String value = request.body();
        if (value == null || value.isEmpty())
          halt(StatusCode.BAD_REQUEST.getCode());

        try {
          client.setString(key, column, value);
        } catch (CassandraClientException e) {
          e.printStackTrace();
          halt(StatusCode.SERVICE_UNAVAILABLE.getCode());
        }
        return "OK";
      }
    });

  }

}
