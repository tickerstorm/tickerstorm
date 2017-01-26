/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.common.config;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
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

  @Test
  public void constructSymbolConfig() throws Exception {

    SymbolConfig config = new SymbolConfig();
    config.symbol.add("*");
    config.interval.add("*");
    config.periods.add("2");

    TransformerConfig tc = new TransformerConfig(com.google.common.collect.Sets.newHashSet(config));

    Set<Integer> periods = tc.findPeriod("TOL", "1m");
    Assert.assertNotNull(periods);
    Assert.assertFalse(periods.isEmpty());
    Assert.assertEquals(periods.iterator().next(), new Integer(2));

  }

}
