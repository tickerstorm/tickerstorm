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

package io.tickerstorm.client;

import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import io.tickerstorm.common.Session;
import io.tickerstorm.common.SessionFactory;
import io.tickerstorm.common.command.ExportModelDataToCSV;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.eventbus.Destinations;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestBacktestRunnerClientContext.class})
public class TestSubmitToH2OITCase {

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  private EventBus notifications;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Autowired
  private EventBus commandBus;

  @Autowired
  private SessionFactory factory;

  @Autowired
  private BacktestRunnerClient client;

  private static final String path = "/tmp/MarketDataFile-2015-10-17T04:19:43.322Z.csv";

  private Session session;

  @Before
  public void setup() throws Exception {
    FileUtils.forceMkdir(new File("/tmp/"));
    session = factory.newSession();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(path));
    session.end();
  }

  // @Test
  public void testSubmitCSVToH2OOnEvent() throws Exception {

    Files.copy(new File("./src/test/resources/data/MarketDataFile-2015-10-17T04:19:43.322Z.csv"), new File(path));

    ExportModelDataToCSV export = new ExportModelDataToCSV(session.stream());
    export.markers.add(Markers.LOCATION.toString());
    export.config.put(Markers.LOCATION.toString(), path);
    commandBus.post(export);

  }

}
