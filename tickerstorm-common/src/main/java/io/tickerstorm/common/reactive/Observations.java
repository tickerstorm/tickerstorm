/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.common.reactive;

import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.service.HeartBeat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

public interface Observations {

  public interface MarketData {

    public static Predicate<Notification> isSaved(String stream) {
      return (marker -> stream.equalsIgnoreCase(marker.stream) && marker.markers.contains(Markers.MARKET_DATA.toString())
          && marker.markers.contains(Markers.SAVE.toString()) && marker.markers.contains(Markers.SUCCESS.toString()));
    }

    public static Predicate<HeartBeat> isAlive() {
      return (h -> h.getService().equalsIgnoreCase("data-service")
          && Instant.now().minus(5, ChronoUnit.SECONDS).isBefore(h.getTimestamp()));
    }

    public interface Ingest {

      public static Predicate<Notification> started =
          (p -> p.is(Markers.FILE.toString()) && p.is(Markers.INGEST.toString()) && p.is(Markers.START.toString()));

      public static Predicate<Notification> finished =
          (p -> p.is(Markers.FILE.toString()) && p.is(Markers.INGEST.toString()) && p.is(Markers.SUCCESS.toString()));

      public static Predicate<Notification> failed =
          (p -> p.is(Markers.FILE.toString()) && p.is(Markers.INGEST.toString()) && p.is(Markers.FAILED.toString()));

    }
  }

  public interface ModelData {

    public static Predicate<Notification> isSaved(String stream) {
      return (n -> stream.equalsIgnoreCase(n.stream) && n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.SAVE.toString())
          && n.is(Markers.SUCCESS.toString()));
    }

    public interface Query {

      public static Predicate<Notification> ended =
          (n -> n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.QUERY.toString()) && n.is(Markers.END.toString()));

      public static Predicate<Notification> started =
          (n -> n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.QUERY.toString()) && n.is(Markers.START.toString()));
    }

    public interface Command {

      public static Predicate<Notification> ended =
          (n -> n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.COMMAND.toString()) && n.is(Markers.END.toString()));

      public static Predicate<Notification> started =
          (n -> n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.COMMAND.toString()) && n.is(Markers.START.toString()));

      public static Predicate<Notification> inProgress =
          (n -> n.is(Markers.MODEL_DATA.toString()) && n.is(Markers.COMMAND.toString()) && n.is(Markers.IN_PROGRESS.toString()));
    }

    public interface Export {

      public static Predicate<Notification> someCsvExportStarted = (f -> f.is(Markers.FILE.toString()) && f.is(Markers.EXPORT.toString())
          && f.is(Markers.CSV.toString()) && f.is(Markers.START.toString()));

      public static Predicate<Notification> someCsvExportFinished = (f -> f.is(Markers.FILE.toString()) && f.is(Markers.EXPORT.toString())
          && f.is(Markers.CSV.toString()) && f.is(Markers.SUCCESS.toString()));

      public static Predicate<Notification> someCsvExportFailed = (f -> f.is(Markers.FILE.toString()) && f.is(Markers.EXPORT.toString())
          && f.is(Markers.CSV.toString()) && f.is(Markers.FAILED.toString()));
    }


  }

  public interface Session {

    public static Predicate<Command> isStart =
        (p -> p.markers.contains(Markers.SESSION.toString()) && p.markers.contains(Markers.START.toString()));

    public static Predicate<Notification> started =
        (p -> p.markers.contains(Markers.SESSION.toString()) && p.markers.contains(Markers.START.toString()) && p.markers.contains(Markers.SUCCESS.toString()));

    public static Predicate<Notification> ended =
        (p -> p.markers.contains(Markers.SESSION.toString()) && p.markers.contains(Markers.END.toString()) && p.markers.contains(Markers.SUCCESS.toString()));

    public static Predicate<Command> isEnd =
        (p -> p.markers.contains(Markers.END.toString()) && p.markers.contains(Markers.SESSION.toString()));
  }


}
