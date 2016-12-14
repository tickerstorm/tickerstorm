package io.tickerstorm.common.config;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TransformerConfig {

  private Node<String> periodsTree = new Node<String>("periods", false);
  private List<SymbolConfig> symbolConfigs = Lists.newArrayList();
  private Map<String, Set<Integer>> periods = Maps.newHashMap();
  private boolean isActive = false;

  public TransformerConfig(List<Map<String, String>> map) throws Exception {

    for (Map<String, String> m : map) {
      SymbolConfig c = new SymbolConfig();
      BeanWrapper wrapper = PropertyAccessorFactory.forBeanPropertyAccess(c);
      wrapper.setAutoGrowNestedPaths(true);
      wrapper.setPropertyValues(m);
      symbolConfigs.add(c);

      periodsTree.addChildren(c.symbol);

      for (String s : c.symbol) {
        periodsTree.getChild(s).addChildren(c.interval);
        for (String i : c.interval) {
          periodsTree.getChild(s).getChild(i).addChildren(c.periods);
        }
      }

    }

    isActive = true;

  }

  public TransformerConfig(Set<SymbolConfig> symbolConfigs) {

    for (SymbolConfig c : symbolConfigs) {
      this.symbolConfigs.add(c);

      periodsTree.addChildren(c.symbol);

      for (String s : c.symbol) {
        periodsTree.getChild(s).addChildren(c.interval);
        for (String i : c.interval) {
          periodsTree.getChild(s).getChild(i).addChildren(c.periods);
        }
      }
    }

    isActive = true;

  }

  public TransformerConfig(boolean isActive) {
    this.isActive = isActive;
  }

  public boolean isActive() {
    return isActive;
  }

  public Set<Integer> findPeriod(String symbol, String interval) {

    String key = (symbol + interval).toLowerCase();

    if (!periods.containsKey(key)) {
      Node<String> n1 = periodsTree.getOtherwiseChild(symbol, "*");

      if (n1 == null)
        throw new IllegalArgumentException("No period configuration found for symbol " + symbol + " or *");

      Node<String> n2 = n1.getOtherwiseChild(interval, "*");

      if (n2 == null)
        throw new IllegalArgumentException(
            "No period configuration found for symbol " + n1.getData() + " with interval " + interval + " or *");

      Set<String> vals = n2.getChildrensData();
      Set<Integer> ints = vals.stream().map(s -> Integer.valueOf(s)).collect(Collectors.toSet());
      periods.put(key, ints);
    }

    return periods.get(key);

  }

}
