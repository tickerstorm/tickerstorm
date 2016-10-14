package io.tickerstorm.common.command;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

import io.tickerstorm.service.HeartBeat;

public interface CompletionTracker {

  public interface MarketData {

    public static Predicate<Notification> isSaved(String stream) {
      return (marker -> stream.equalsIgnoreCase(marker.stream) && marker.markers.contains(Markers.MARKET_DATA.toString())
          && marker.markers.contains(Markers.SAVE.toString()) && marker.markers.contains(Markers.SUCCESS.toString()));
    }

    public static Predicate<HeartBeat> isAlive() {
      return (h -> h.getService().equalsIgnoreCase("data-service")
          && Instant.now().minus(5, ChronoUnit.SECONDS).isBefore(h.getTimestamp()));
    }

  }

  public interface ModelData {

    public interface Export {

      public static Predicate<Notification> someCsvExportStarted = (f -> f.is(Markers.FILE.toString()) && f.is(Markers.EXPORT.toString())
          && f.is(Markers.CSV.toString()) && f.is(Markers.START.toString()));

      public static Predicate<Notification> someCsvExportFinished = (f -> f.is(Markers.FILE.toString()) && f.is(Markers.EXPORT.toString())
          && f.is(Markers.CSV.toString()) && f.is(Markers.SUCCESS.toString()));
    }

    public static Predicate<Notification> isSaved(String stream) {
      return (n -> stream.equalsIgnoreCase(n.stream) && n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.SAVE.toString())
          && n.is(Markers.SUCCESS.toString()));
    }

    public static Predicate<Notification> someModelDataQueryEnded =
        (n -> n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.QUERY.toString()) && n.is(Markers.END.toString()));

    public static Predicate<Notification> someModelDataQueryStarted =
        (n -> n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.QUERY.toString()) && n.is(Markers.START.toString()));


  }

  public interface Session {
    public static Predicate<Command> isStart =
        (p -> p.markers.contains(Markers.SESSION.toString()) && p.markers.contains(Markers.START.toString()));

    public static Predicate<Command> isEnd =
        (p -> p.markers.contains(Markers.END.toString()) && p.markers.contains(Markers.SESSION.toString()));
  }

  public interface Ingest {

    public static Predicate<Notification> someIngestStarted =
        (p -> p.is(Markers.FILE.toString()) && p.is(Markers.INGEST.toString()) && p.is(Markers.START.toString()));

    public static Predicate<Notification> someIngestFinished =
        (p -> p.is(Markers.FILE.toString()) && p.is(Markers.INGEST.toString()) && p.is(Markers.SUCCESS.toString()));

    public static Predicate<Notification> someIngestFailed =
        (p -> p.is(Markers.FILE.toString()) && p.is(Markers.INGEST.toString()) && p.is(Markers.FAILED.toString()));

  }

}
