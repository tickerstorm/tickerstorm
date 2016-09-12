package io.tickerstorm.common.command;

import io.tickerstorm.common.data.query.ModelDataQuery;

@SuppressWarnings("serial")
public class ExportModelDataToCSV extends Command {
  
  public static final String EXPORT_TO_CSV_MARKER = "model_data_export_csv";
  public static final String EXPORT_TO_CSV_COMPLETE_MARKER = "model_data_export_complete_csv";
  public static final String FILE_LOCATION = "model_data_export_csv_location";
  
  public ModelDataQuery modelQuery;

  public ExportModelDataToCSV(String stream) {
    super(stream);
    this.markers.add(EXPORT_TO_CSV_COMPLETE_MARKER);
    this.modelQuery = new ModelDataQuery(stream);
  }
  
  public ExportModelDataToCSV(String stream, ModelDataQuery q) {
    super(stream);
    this.modelQuery = q;
    this.markers.add(EXPORT_TO_CSV_COMPLETE_MARKER);
  }

}
