package io.tickerstorm.data.export;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.data.dao.influxdb.InfluxModelDataDao;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.prefs.CsvPreference;

@Service
public class ModelDataExporter {

  public final static String CACHE_KEY_SUFFIX = "_csv_field_names";
  private final static Logger logger = LoggerFactory.getLogger(ModelDataExporter.class);
  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @Autowired
  private InfluxModelDataDao dao;

  @PostConstruct
  private void init() {
    commandBus.register(this);
  }

  @PreDestroy
  private void destroy() {
    commandBus.unregister(this);
  }

  @Subscribe
  public void onCommand(ExportModelDataToCSV command) {

    final List<String> fieldNames = Lists.newArrayList(dao.allHeaders(command.getStream()));
    final List<Set<Field<?>>> dtos = dao.newSelect(command.modelQuery.getStream()).between(command.modelQuery.from, command.modelQuery.until).select();

    if (fieldNames.isEmpty() || dtos.isEmpty()) {

      Notification n = new Notification(command);
      n.addMarker(Markers.FAILED.toString());
      n.addMarker(Markers.MESSAGE.toString());

      if (fieldNames.isEmpty()) {
        n.properties.put(Markers.MESSAGE.toString(), "No headers found to export for stream " + command.getStream());
        logger.error("No header information found to export for stream " + command.getStream());
      }

      if (dtos.isEmpty()) {
        n.properties.put(Markers.MESSAGE.toString(), "No data found to export for stream " + command.getStream());
        logger.error("No data found to export for stream " + command.getStream());
      }

      notificationBus.post(n);
      return;
    }

    File file = createTempFile((String) command.config.get(Markers.LOCATION.toString()));
    logger.debug("Found " + fieldNames + " as header columns for stream " + command.getStream() + ". Exporting model data to "
        + file.getAbsolutePath());

    final CellProcessor[] cellProcessor = new CellProcessor[fieldNames.size()];
    Arrays.fill(cellProcessor, new Optional());

    try (CsvMapWriter mapWriter = new CsvMapWriter(new FileWriter(file), CsvPreference.STANDARD_PREFERENCE)) {

      Notification n = new Notification(command);
      n.addMarker(Markers.START.toString());
      notificationBus.post(n);

      logger.info("Exporting " + dtos.size() + " rows to " + file.getAbsolutePath());

      final String[] header = fieldNames.toArray(new String[]{});
      Arrays.sort(header);
      mapWriter.writeHeader(header);
      final Map<String, Object> row = new HashMap<>();

      dtos.stream().forEach(s -> {

        s.stream().forEach(f -> {
          row.put(f.getName(), f.getValue());
        });

        try {
          mapWriter.write(row, header, cellProcessor);
        } catch (Exception e2) {
          logger.error(e2.getMessage(), e2);
        } finally {
          row.clear();
        }
      });

      logger.info("Done exporting model data to " + file.getAbsolutePath());

      n = new Notification(command);
      n.addMarker(Markers.SUCCESS.toString());
      n.addMarker(Markers.LOCATION.toString());
      n.properties.put(Markers.LOCATION.toString(), file.getAbsolutePath());
      notificationBus.post(n);

    } catch (Exception ex) {

      Notification n = new Notification(command);
      n.addMarker(Markers.FAILED.toString());
      n.addMarker(Markers.LOCATION.toString());
      n.properties.put(Markers.LOCATION.toString(), file.getAbsolutePath());
      notificationBus.post(n);

      logger.error(ex.getMessage(), ex);
    }
  }

  private File createTempFile(String locatoion) {
    File file = new File(locatoion);

    try {

      if (!StringUtils.isEmpty(locatoion)) {
        if (!file.exists()) {
          Files.createParentDirs(file);
          Files.touch(file);
        }
      } else {
        file = File.createTempFile(UUID.randomUUID().toString(), "csv");
        Files.createParentDirs(file);
        Files.touch(file);
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return file;
  }
}
