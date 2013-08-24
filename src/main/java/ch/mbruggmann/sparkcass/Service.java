package ch.mbruggmann.sparkcass;

import spark.Request;
import spark.Response;
import spark.Route;

import static spark.Spark.get;

public class Service {

  public static void main(String[] args) {

    get(new Route("/hello") {
      @Override
      public Object handle(Request request, Response response) {
        return "Hello World!";
      }
    });

  }

}
