package io.tickerstorm.common.config;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.springframework.core.io.DefaultResourceLoader;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

public class TestTransformerConfig {

  @Test
  public void testConfigureViaYaml() throws Exception {

    InputStream content = new DefaultResourceLoader().getResource("classpath:yml/sample.yml").getInputStream();
    Map<String, Object> vals = new Yaml().loadAs(content, Map.class);

    Map<String, List<Map>> transformers = (Map) vals.get("transformers");

    List<Map<String, String>> numchance = (List) transformers.get("numeric-change");
    TransformerConfig config = new TransformerConfig(numchance);
  
    Assert.assertEquals(config.findPeriod("Goog", "1m"), com.google.common.collect.Sets.newHashSet(10));
    Assert.assertEquals(config.findPeriod("Goog", "1d"), com.google.common.collect.Sets.newHashSet(5, 10));
    Assert.assertEquals(config.findPeriod("Tol", "1m"), com.google.common.collect.Sets.newHashSet(10, 20, 30));
    Assert.assertEquals(config.findPeriod("tol", "xzy"), com.google.common.collect.Sets.newHashSet(10, 20, 30));

    numchance = (List) transformers.get("basic-stats");
    config = new TransformerConfig(numchance);
    Assert.assertEquals(config.findPeriod("Goog", "1m"), com.google.common.collect.Sets.newHashSet(1, 10, 15, 30, 60, 90));
    Assert.assertEquals(config.findPeriod("GOOG", "1d"), com.google.common.collect.Sets.newHashSet(1, 10, 15, 30, 60, 90));
    Assert.assertEquals(config.findPeriod("TOL", "1m"), com.google.common.collect.Sets.newHashSet(1, 10, 15, 30, 60, 90));
    Assert.assertEquals(config.findPeriod("tol", "xus"), com.google.common.collect.Sets.newHashSet(1, 10, 15, 30, 60, 90));


  }

}
