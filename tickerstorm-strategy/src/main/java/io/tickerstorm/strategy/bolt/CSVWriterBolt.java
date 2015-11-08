package io.tickerstorm.strategy.bolt;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Throwables;
import com.google.common.io.Files;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.tickerstorm.common.entity.Command;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.Marker;
import io.tickerstorm.common.entity.Markers;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.entity.Notification;
import io.tickerstorm.common.model.Fields;
import io.tickerstorm.strategy.util.TupleUtil;

@Component
@SuppressWarnings("serial")
public class CSVWriterBolt extends BaseBolt {

  private final static Logger logger = LoggerFactory.getLogger(CSVWriterBolt.class);
  private Writer outputFile = null;
  private File file = null;
  private AtomicBoolean firstLine = new AtomicBoolean(true);

  @Override
  public void process(Tuple tuple) {

    if (tuple.contains(Fields.MARKER.fieldName())) {

      Marker m = (Marker) tuple.getValueByField(Fields.MARKER.fieldName());

      if (Markers.is(m, Markers.SESSION_START) && Command.class.isAssignableFrom(m.getClass())) {

        Command c = (Command) m;

        String fileName = (String) c.config.get("output.file.csv.path");
        firstLine = new AtomicBoolean(true);

        if (StringUtils.isEmpty(fileName)) {
          fileName = "/tmp/" + UUID.randomUUID().toString() + ".csv";
        }

        file = new File(fileName);

        try {

          if (file.exists()) {
            fileName = Files.getNameWithoutExtension(fileName).concat("-" + Instant.now().getEpochSecond())
                .concat(Files.getFileExtension(fileName));
          }

          logger.info("Creating CSV file " + fileName);
          Files.createParentDirs(file);
          Files.touch(file);
          outputFile = Files.newWriter(file, Charset.forName("UTF-8"));

        } catch (Exception e) {
          Throwables.propagate(e);
        }
      } else if (Markers.is(m, Markers.SESSION_END) && Command.class.isAssignableFrom(m.getClass())) {

        logger.info("Flush to CSV complete");

        try {
          outputFile.flush();
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        }

        Notification n = new Notification();
        n.setSource(this.getClass().getSimpleName());
        n.addMarker(Markers.CSV_CREATED.toString());
        n.getProperties().put("output.file.csv.path", file.getAbsolutePath());
        emit(n);
      }

    }

    if (tuple.contains(Fields.MARKETDATA.fieldName())) {

      List<Field<?>> columns = TupleUtil.sortFields(TupleUtil.listFields(tuple));

      try {

        if (firstLine.get()) {
          outputFile.append(writeHeader(columns));
          firstLine.set(false);
        }

        outputFile.append(writeLine(columns));
        outputFile.flush();

      } catch (IOException e) {
        logger.error(e.getMessage(), e);
      }
    }
    ack(tuple);
  }

  private String writeHeader(List<Field<?>> sorted) {

    StringBuffer headerLine = new StringBuffer();

    for (Field<?> header : sorted) {
      headerLine.append(header.getName()).append(",");
    }

    String header = org.apache.commons.lang3.StringUtils.removeEnd(headerLine.toString(), ",");
    header = header.concat("\n");
    return header;

  }

  private String writeLine(List<Field<?>> sorted) {
    StringBuffer valueLine = new StringBuffer();

    for (Field<?> header : sorted) {
      valueLine.append(header.getValue()).append(",");
    }

    String line = org.apache.commons.lang3.StringUtils.removeEnd(valueLine.toString(), ",");
    line = line.concat("\n");
    return line;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new backtype.storm.tuple.Fields("notification.output.file.csv.path"));
  }

  @Override
  public void cleanup() {
    super.cleanup();
    IOUtils.closeQuietly(outputFile);
  }

}
