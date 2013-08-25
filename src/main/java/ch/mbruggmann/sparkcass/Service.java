package ch.mbruggmann.sparkcass;

import ch.mbruggmann.sparkcass.cassandra.AstyanaxCassandraClient;
import ch.mbruggmann.sparkcass.cassandra.CassandraClientException;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.File;

import static spark.Spark.get;
import static spark.Spark.post;

public class Service {

  public static void main(String[] args) {

    final Config config = Config.fromFile(new File("sparkcass.conf"));
    final AstyanaxCassandraClient client = new AstyanaxCassandraClient(config);

    get(new Route("/:key/:column") {
      @Override
      public Object handle(Request request, Response response) {
        final String key = request.params(":key");
        final String column = request.params(":column");
        if (key.isEmpty() || column.isEmpty()) {
          return errorResponse(response, StatusCode.BAD_REQUEST);
        }

        final String value;
        try {
          value = client.getString(key, column);
        } catch (CassandraClientException e) {
          e.printStackTrace();
          return errorResponse(response, StatusCode.SERVICE_UNAVAILABLE);
        }

        if (value == null) {
          return errorResponse(response, StatusCode.NOT_FOUND);
        }

        return valueResponse(response, value);
      }
    });

    post(new Route("/:key/:column") {
      @Override
      public Object handle(Request request, Response response) {
        final String key = request.params(":key");
        final String column = request.params(":column");
        if (key.isEmpty() || column.isEmpty()) {
          return errorResponse(response, StatusCode.BAD_REQUEST);
        }

        final String value = request.body();
        if (value == null || value.isEmpty()) {
          return errorResponse(response, StatusCode.BAD_REQUEST);
        }

        try {
          client.setString(key, column, value);
        } catch (CassandraClientException e) {
          e.printStackTrace();
          return errorResponse(response, StatusCode.SERVICE_UNAVAILABLE);
        }

        return successResponse(response);
      }
    });

  }

  private static Object valueResponse(Response response, String value) {
    response.type("text/plain");
    response.status(StatusCode.OK.getCode());
    return value;
  }

  private static Object successResponse(Response response) {
    response.type("text/plain");
    response.status(StatusCode.OK.getCode());
    return "OK";
  }

  private static Object errorResponse(Response response, StatusCode status) {
    response.type("text/plain");
    response.status(status.getCode());
    return status.name();
  }

}
