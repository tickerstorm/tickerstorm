package io.tickerstorm.common.config;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class SymbolConfig implements Serializable {

  public List<String> symbol = Lists.newArrayList();
  public List<String> interval = Lists.newArrayList();
  public List<String> periods = Lists.newArrayList();
  public String source;

  public List<String> getSymbol() {
    return symbol;
  }

  public void setSymbol(List<String> symbol) {
    this.symbol = symbol;
  }

  public List<String> getInterval() {
    return interval;
  }

  public void setInterval(List<String> interval) {
    this.interval = interval;
  }

  public List<String> getPeriods() {
    return periods;
  }

  public void setPeriods(List<String> periods) {
    this.periods = periods;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }



}
