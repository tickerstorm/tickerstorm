package io.tickerstorm.common.data.query;

import java.time.LocalDateTime;
import java.util.UUID;

@SuppressWarnings("serial")
public class ModelDataQuery implements DataFeedQuery {
  
  public enum OrderBy {
    ASC, DESC;
  }
  
  public final String id = UUID.randomUUID().toString();
  public String stream;
  public LocalDateTime from = LocalDateTime.MIN;
  public LocalDateTime until = LocalDateTime.now();
  public OrderBy sort = OrderBy.DESC;
  
  public ModelDataQuery(String stream) {
    this.stream = stream;
  }
  
  @Override
  public String id() {
    return id;
  }

}
