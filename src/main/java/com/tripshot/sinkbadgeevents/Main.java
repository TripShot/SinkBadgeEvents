package com.tripshot.sinkbadgeevents;

import java.io.IOException;

public class Main {

  public static void main(String args[]) throws IOException {

    SinkBadgeEvents badgeEvents = new SinkBadgeEvents("00000000-0000-0000-0000-000000000000", "YOUR_SECRET", "https://something.tripshot.com");

    badgeEvents.startSink(System.out::println);
  }

}
