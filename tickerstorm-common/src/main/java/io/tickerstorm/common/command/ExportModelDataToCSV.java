package io.tickerstorm.common.command;

import io.tickerstorm.common.reactive.CompletionTracker.ModelData.Export;
import io.tickerstorm.common.reactive.Notification;
import java.util.function.Predicate;

@SuppressWarnings("serial")
public class ExportModelDataToCSV extends Command {

  public ModelDataQuery modelQuery;

  public ExportModelDataToCSV(String stream) {
    super(stream, "data.modeldata.export.csv");
    this.markers.add(Markers.FILE.toString());
    this.markers.add(Markers.EXPORT.toString());
    this.markers.add(Markers.CSV.toString());
    this.modelQuery = new ModelDataQuery(stream);
  }

  public ExportModelDataToCSV(String stream, ModelDataQuery q) {
    super(stream, "data.modeldata.export.csv");
    this.modelQuery = q;
    this.markers.add(Markers.FILE.toString());
    this.markers.add(Markers.EXPORT.toString());
    this.markers.add(Markers.CSV.toString());
  }

  public Predicate<Notification> isDone() {
    return Export.someCsvExportFinished
        .and(n -> this.getStream().equalsIgnoreCase(n.getStream()) && n.id.equals(this.id));
  }

  public Predicate<Notification> started() {
    return Export.someCsvExportStarted
        .and(n -> this.getStream().equalsIgnoreCase(n.getStream()) && n.id.equals(this.id));
  }

  @Override
  public boolean isValid() {
    return (super.validate() && this.modelQuery != null);
  }

}
