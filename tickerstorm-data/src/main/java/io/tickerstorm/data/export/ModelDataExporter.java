package io.tickerstorm.data.export;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

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

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;

import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.reactive.Notification;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.data.dao.cassandra.CassandraModelDataDao;
import io.tickerstorm.data.dao.cassandra.CassandraModelDataDto;
import net.sf.ehcache.Element;

@Service
public class ModelDataExporter {

  private final static Logger logger = LoggerFactory.getLogger(ModelDataExporter.class);

  public final static String CACHE_KEY_SUFFIX = "_csv_field_names";

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandBus;

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notificationBus;

  @Autowired
  private CassandraModelDataDao cassandraDao;

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

    Element e = CacheManager.getInstance(command.getStream()).get(ModelDataExporter.CACHE_KEY_SUFFIX);

    Set<String> fieldNames = new HashSet<>();
    if (e != null) {
      fieldNames = (Set<String>) e.getObjectValue();
      if (fieldNames.isEmpty()) {

        Notification n = new Notification(command);
        n.addMarker(Markers.FAILED.toString());
        n.addMarker(Markers.MESSAGE.toString());
        n.properties.put(Markers.MESSAGE.toString(), "No header information found to export for stream " + command.getStream());
        notificationBus.post(n);

        logger.error("No header information found to export for stream " + command.getStream());
        return;
      }
    }

    File file = createTempFile((String) command.config.get(Markers.LOCATION.toString()));
    logger.debug("Found " + fieldNames + " as header columns for stream " + command.getStream() + ". Exporting model data to "
        + file.getAbsolutePath());

    Notification n = new Notification(command);
    n.addMarker(Markers.START.toString());
    notificationBus.post(n);

    final CellProcessor[] cellProcessor = new CellProcessor[fieldNames.size()];
    Arrays.fill(cellProcessor, new Optional());

    try (CsvMapWriter mapWriter = new CsvMapWriter(new FileWriter(file), CsvPreference.STANDARD_PREFERENCE)) {

      final String[] header = fieldNames.toArray(new String[] {});
      mapWriter.writeHeader(header);
      Map<String, Object> row = new HashMap<>();

      Stream<CassandraModelDataDto> dtos = cassandraDao
          .findByStreamAndTimestampIsBetween(command.modelQuery.getStream(), command.modelQuery.from, command.modelQuery.until).stream();

      dtos.forEach(d -> {

        try {
          d.asFields().forEach(f -> {
            row.put(f.getName(), f.getValue());
          });

          mapWriter.write(row, header, cellProcessor);
        } catch (Exception e2) {
          logger.error(e2.getMessage(), e2);
        }
      });

      logger.info("Exported model data to " + file.getAbsolutePath());

      n = new Notification(command);
      n.addMarker(Markers.SUCCESS.toString());
      n.addMarker(Markers.LOCATION.toString());
      n.properties.put(Markers.LOCATION.toString(), file.getAbsolutePath());
      notificationBus.post(n);

    } catch (Exception ex) {

      n = new Notification(command);
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
