package com.tripshot.sinkbadgeevents;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.Key;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.escape.CharEscapers;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

public class SinkBadgeEvents {

  private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  public static class AccessTokenRequest {
    @SuppressWarnings("unused")
    @Key
    private String appId;

    @SuppressWarnings("unused")
    @Key
    private String secret;

    public AccessTokenRequest(String appId, String secret) {
      this.appId = appId;
      this.secret = secret;
    }
  }

  public static class AccessTokenResponse {
    @SuppressWarnings("unused")
    @Key
    private String accessToken;
  }

  public static class BadgeReport {

    @Key
    private List<BadgeEvent> badgeEvents;

    @Key
    private String cursor;
  }

  public static class BadgeEvent {

    @Key
    private String riderId;

    @Key
    private String at;

    @Key
    private LatLng location;

    @Key
    private String stopName;

    @Key
    private String vehicleName;

    @Key
    private String rideName;

    public String getRiderId() {
      return riderId;
    }

    public Instant getAt() {
      return ISODateTimeFormat.dateHourMinuteSecondFraction().parseDateTime(at).toInstant();
    }

    public LatLng getLocation() {
      return location;
    }

    public String getStopName() {
      return stopName;
    }

    public String getVehicleName() {
      return vehicleName;
    }

    public String getRideName() {
      return rideName;
    }

    @Override
    public String toString() {
      return "BadgeEvent{" +
        "riderId='" + riderId + '\'' +
        ", at=" + at +
        ", location=" + location +
        ", stopName='" + stopName + '\'' +
        ", vehicleName='" + vehicleName + '\'' +
        ", rideName='" + rideName + '\'' +
        '}';
    }
  }

  public static class LatLng {

    @Key
    private double lg;

    @Key
    private double lt;

    public double getLong() {
      return lg;
    }

    public double getLat() {
      return lt;
    }

    @Override
    public String toString() {
      return "LatLng{" +
        "lg=" + lg +
        ", lt=" + lt +
        '}';
    }
  }


  private void watchForAndSendAlerts(HttpRequestFactory requestFactory, Consumer<BadgeEvent> sink) throws IOException {

    String cursor = null;
    Instant startTime = new Instant();

    //noinspection InfiniteLoopStatement
    while ( true ) {
      BadgeReport report;

      if ( cursor != null ) {
        report = fetchBadgeReport(requestFactory, cursor, null);
      } else {
        report = fetchBadgeReport(requestFactory, null, startTime);
      }

      for ( BadgeEvent event : report.badgeEvents ) {
        try {
          sink.accept(event);
        } catch ( Throwable t ) {
          // Just log the problem, but keep on going to allow other alerts to fire.
          t.printStackTrace();
        }
      }

      if ( report.badgeEvents.isEmpty() ) {
        // If there were no events in last poll, back off for a bit ...
        try {
          Thread.sleep(5000);
        } catch ( InterruptedException t) {
          throw Throwables.propagate(t);
        }
      } else {
        // ... but if there were events, try again immediately until there are no more ready.
        cursor = report.cursor;
      }
    }
  }

  private BadgeReport fetchBadgeReport(HttpRequestFactory requestFactory, String cursor, Instant startTime) throws IOException {
    Preconditions.checkArgument((cursor == null) != (startTime == null), "exactly one of 'cursor' or 'startTime' must be non-null");

    // Getting a new token for every request is not efficient as a token can be reused until a 401 is returned. But keeping it simple for the example.
    String accessToken = getAccessToken(requestFactory);

    String url = baseUrl + "/v1/badgeReport";
    if ( cursor != null ) {
      url = url + "?cursor=" + CharEscapers.escapeUriPath(cursor);
    } else {
      url = url + "?startTime=" + ISODateTimeFormat.dateHourMinuteSecondFraction().print(startTime);
      url = url + "&endTime=" + ISODateTimeFormat.dateHourMinuteSecondFraction().print(new Instant());
    }

    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));
    request.setHeaders(new HttpHeaders().setAuthorization("Bearer " + accessToken));

    return request.execute().parseAs(BadgeReport.class);
  }


  private String getAccessToken(HttpRequestFactory requestFactory) throws IOException {

    HttpRequest accessTokenRequest =
      requestFactory.buildPostRequest(new GenericUrl(baseUrl + "/v1/accessToken"), new JsonHttpContent(JSON_FACTORY, new AccessTokenRequest(appId, secret)));
    AccessTokenResponse accessTokenResponse = accessTokenRequest.execute().parseAs(AccessTokenResponse.class);

    return accessTokenResponse.accessToken;
  }

  private final String appId;
  private final String secret;
  private final String baseUrl;

  public SinkBadgeEvents(String appId, String secret, String baseUrl) {
    this.appId = appId;
    this.secret = secret;
    this.baseUrl = baseUrl;
  }


  // Starts the sink in a separate thread. Should only be called once.
  public void startSink(Consumer<BadgeEvent> sink) throws IOException {

    new Thread(() -> {
      HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(request -> request.setParser(new JsonObjectParser(JSON_FACTORY)));

      try {
        watchForAndSendAlerts(requestFactory, sink);
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }).start();

  }

}
