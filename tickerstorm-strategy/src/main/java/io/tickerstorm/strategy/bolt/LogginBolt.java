package io.tickerstorm.strategy.bolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import backtype.storm.tuple.Tuple;

@Component
@SuppressWarnings("serial")
public class LogginBolt extends BaseBolt {

  private final static Logger logger = LoggerFactory.getLogger(LogginBolt.class);

  @Override
  protected void process(Tuple tuple) {

    StringBuffer buf = new StringBuffer("Tuple contains ");
    for (String f : tuple.getFields()) {
      buf = buf.append(f + ",");
    }
    logger.debug(buf.toString());
    ack(tuple);
  }

}
