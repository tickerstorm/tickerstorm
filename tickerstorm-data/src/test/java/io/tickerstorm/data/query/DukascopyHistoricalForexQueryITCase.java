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
import java.math.BigDecimal;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestMarketDataServiceConfig.class})
public class DukascopyHistoricalForexQueryITCase extends BaseDataQueryITCase {

  @Before
  public void setup() throws Exception {
    verifier = new DownloadGloabForextVerification();
    FileUtils.forceMkdir(new File(Locations.FILE_DROP_LOCATION + "/Dukascopy"));
    super.setup();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(Locations.FILE_DROP_LOCATION + "/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv"));
    dao.deleteByStream("Dukascopy");
    super.tearDown();
  }

  @Test
  public void parseGloabForext() throws Exception {

    Files.copy(new ClassPathResource("/data/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv").getFile(),
        new FileSystemResource(Locations.FILE_DROP_LOCATION + "/Dukascopy/AUDCAD_Candlestick_1_m_BID_01.06.2015-06.06.2015.csv").getFile());

    Thread.sleep(8000);

    Long daoCount = dao.count("Dukascopy");
    Assert.assertEquals(daoCount, new Long(52860));
    Assert.assertEquals(count.get(), 52860L);

  }

  private class DownloadGloabForextVerification {

    @Subscribe
    public void onEvent(MarketData md) {

      assertNotNull(md.getSymbol());
      assertEquals(md.getStream(), "Dukascopy");
      assertNotNull(md.getTimestamp());

      Bar c = (Bar) md;
      assertNotNull(c.close);
      assertTrue(c.close.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.open);
      assertTrue(c.open.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.low);
      assertTrue(c.low.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.high);
      assertTrue(c.high.compareTo(BigDecimal.ZERO) > 0);
      assertNotNull(c.volume);
      assertEquals(c.interval, Bar.MIN_1_INTERVAL);
      count.incrementAndGet();

    }

  }
}
