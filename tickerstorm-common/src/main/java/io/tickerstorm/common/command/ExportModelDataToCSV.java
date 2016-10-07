package io.tickerstorm.common.command;

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

  @Override
  public boolean isValid() {
    return (super.validate() && this.modelQuery != null);
  }

}
