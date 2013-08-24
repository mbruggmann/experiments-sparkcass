package ch.mbruggmann.sparkcass;

/**
 * Maps selected HTTP status codes.
 */
public enum StatusCode {

  OK(200),
  BAD_REQUEST(400),
  NOT_FOUND(404),
  SERVICE_UNAVAILABLE(503);

  private final int code;

  private StatusCode(int code) {
    this.code = code;
  }

  public int getCode() {
    return this.code;
  }

}
