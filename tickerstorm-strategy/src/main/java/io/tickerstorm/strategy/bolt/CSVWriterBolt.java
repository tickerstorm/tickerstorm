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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.storm.commons.lang.StringUtils;
import org.apache.storm.guava.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.io.Files;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import io.tickerstorm.entity.Field;
import io.tickerstorm.entity.MarketData;

@Component
@SuppressWarnings("serial")
public class CSVWriterBolt extends BaseRichBolt {

  private final static Logger logger = LoggerFactory.getLogger(CSVWriterBolt.class);
  private OutputCollector coll;
  private Writer outputFile = null;
  private AtomicBoolean firstLine = new AtomicBoolean(true);

  @Override
  public void execute(Tuple tuple) {

    List<Field<?>> columns = new ArrayList<>();

    for (Object o : tuple.getValues()) {

      if (MarketData.class.isAssignableFrom(o.getClass())) {

        columns.addAll(((MarketData) o).getFields());

      } else if (Field.class.isAssignableFrom(o.getClass())) {

        columns.add((Field<?>) o);

      } else if (Collection.class.isAssignableFrom(o.getClass())) {

        for (Object i : (Collection<?>) o) {
          if (Field.class.isAssignableFrom(i.getClass())) {
            columns.add((Field<?>) i);
          }
        }

      }
    }

    columns.sort(new Comparator<Field<?>>() {
      @Override
      public int compare(Field<?> o1, Field<?> o2) {
        return (o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase()));
      }
    });

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

    coll.ack(tuple);
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
  public void prepare(Map config, TopologyContext arg1, OutputCollector arg2) {
    this.coll = arg2;
    String fileName = (String) config.get("output.file.csv.name");
    firstLine = new AtomicBoolean(true);

    if (!StringUtils.isEmpty(fileName)) {
      File file = new File(fileName);

      try {

        if (file.exists()) {

          fileName =
              Files.getNameWithoutExtension(fileName).concat("-" + Instant.now().getEpochSecond())
                  .concat(Files.getFileExtension(fileName));
        }

        Files.createParentDirs(file);
        Files.touch(file);
        outputFile = Files.newWriter(file, Charset.forName("UTF-8"));

      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer arg0) {
    // none
  }

  @Override
  public void cleanup() {
    super.cleanup();
    IOUtils.closeQuietly(outputFile);
  }

}
