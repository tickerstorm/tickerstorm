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

package io.tickerstorm.data.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.eventbus.Subscribe;
import com.google.common.io.Files;
import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.data.TestMarketDataServiceConfig;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class GoogleDataQueryITCase extends BaseDataQueryITCase {

  GoogleDataQuery query;



  @Before
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Google"));
    verifier = new GoogleDataVerifier();
    super.setup();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));
    mddao.newDelete().bySource("Google").delete();
    super.tearDown();
  }

  @Test
  public void downloadGloabForext() throws Exception {

    Long daoCount = mddao.newCount(Bar.TYPE.toLowerCase()).count();
    Assert.assertEquals( new Long(0), daoCount);

    query = new GoogleDataQuery("TOL");
    client.query(query);

    Thread.sleep(2000);

    daoCount = mddao.newCount(Bar.TYPE.toLowerCase()).count();
    assertTrue(daoCount > 0);

  }

  @Test
  public void testParseGoogleFile() throws Exception {

    Long daoCount = mddao.newCount(Bar.TYPE.toLowerCase()).bySource("Google").count();
    Assert.assertEquals(daoCount, new Long(0));

    Files.copy(loader.getResource("classpath:/data/Google/TOL.csv").getFile(), new File(Locations.FILE_DROP_LOCATION + "/Google/TOL.csv"));

    Thread.sleep(2000);

    daoCount = mddao.newCount(Bar.TYPE.toLowerCase()).bySource("Google").count();
    assertEquals(count.get(), 5792);
    assertEquals(daoCount, new Long(5792));
  }


  private class GoogleDataVerifier {

    @Subscribe
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getStream(), "Google");
      assertNotNull(md.getTimestamp());

      Bar c = (Bar) md;
      assertNotNull(c.close);
      assertTrue(c.close.longValue() > 0);
      assertNotNull(c.open);
      assertTrue(c.open.longValue() > 0);
      assertNotNull(c.low);
      assertTrue(c.low.longValue() > 0);
      assertNotNull(c.high);
      assertTrue(c.high.longValue() > 0);
      assertNotNull(c.volume);
      assertTrue(c.volume.longValue() > 0);
      assertEquals(c.interval, Bar.MIN_1_INTERVAL);
      count.getAndIncrement();

    }

  }
}
