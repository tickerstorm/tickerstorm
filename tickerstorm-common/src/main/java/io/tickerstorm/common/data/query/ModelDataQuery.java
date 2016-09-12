package io.tickerstorm.common.data.query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@SuppressWarnings("serial")
public class ModelDataQuery implements DataFeedQuery {



  public enum OrderBy {
    ASC, DESC;
  }

  public final String id = UUID.randomUUID().toString();
  public String stream;
  public Instant from = Instant.now().minus(365, ChronoUnit.DAYS);
  public Instant until = Instant.now();
  public OrderBy sort = OrderBy.DESC;

  public ModelDataQuery(String stream) {
    this.stream = stream;
  }

  @Override
  public String id() {
    return id;
  }



}
