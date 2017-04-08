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

package io.tickerstorm.data;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLauncher {

  private static final Logger logger = LoggerFactory.getLogger(ServiceLauncher.class);
  private static String DATA_SERVICE_JAR =
      "/home/kkarski/.m2/repository/io/tickerstorm/data-service/1.0.0-SNAPSHOT/data-service-1.0.0-SNAPSHOT-exec.jar";
  private static String STRATEGY_SERVICE_JAR =
      "/home/kkarski/.m2/repository/io/tickerstorm/strategy-service/1.0.0-SNAPSHOT/strategy-service-1.0.0-SNAPSHOT-exec.jar";
  private Process marketDataShell;
  private Process strategyShell;

  public static void main(String args[]) throws Exception {

    ServiceLauncher launcher = new ServiceLauncher();
    launcher.launchMarketDataService(false, 0, "/tmp/tickerstorm/data-service/monitor");
    launcher.launchStrategyService(false, 0);

  }

  public void launchMarketDataService(boolean debug, int port, final String monitorPath) {

    Runnable run = new Runnable() {
      @Override
      public void run() {

        if (debug && !available(port)) {
          logger.warn("Unable to start market data service, port " + port + " already taken. Serivce is probably running.");
          return;
        }

        List<String> commands = Lists.newArrayList("/usr/bin/java", "-jar");

        if (debug) {
          commands.add("-Xdebug");
          commands.add("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=" + port);
        }
        commands.add(DATA_SERVICE_JAR);

        if (!StringUtils.isEmpty(monitorPath)) {
          commands.add("-Dservice.data.monitor.location=" + monitorPath);
        }

        logger.info("Starting market data service using command " + commands.toString());

        ProcessBuilder builder = new ProcessBuilder(commands);
        //builder.redirectErrorStream(true);

        try {
          marketDataShell = builder.start();

          // To capture output from the shell
          InputStream shellIn = marketDataShell.getInputStream();
          IOUtils.copy(shellIn, System.out);

        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        } catch (Error e) {
          logger.error(e.getMessage(), e);
        }
      }
    };

    Executors.newSingleThreadExecutor().execute(run);

  }

  public void killMarketDataService() {
    try {
      if (marketDataShell != null && marketDataShell.isAlive()) {
        marketDataShell.destroyForcibly();
        int shellExitStatus = marketDataShell.waitFor();
        logger.info("Exit status" + shellExitStatus);
        marketDataShell.getInputStream().close();
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  public void killStrategyService() {
    try {
      if (strategyShell != null && strategyShell.isAlive()) {
        strategyShell.destroyForcibly();
        int shellExitStatus = strategyShell.waitFor();
        logger.info("Exit status" + shellExitStatus);
        strategyShell.getInputStream().close();
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  public void launchStrategyService(boolean debug, int port) {
    Runnable run = new Runnable() {
      @Override
      public void run() {

        if (debug && !available(port)) {
          logger.warn("Unable to start strategy service, port " + port + " already taken. Serivce is probably running.");
          return;
        }

        List<String> commands = Lists.newArrayList("/usr/bin/java", "-jar");

        if (debug) {
          commands.add("-Xdebug");
          commands.add("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=" + port);
        }

        commands.add(STRATEGY_SERVICE_JAR);

        logger.info("Starting strategy service using command " + commands.toString());

        ProcessBuilder builder = new ProcessBuilder(commands);
        //builder.redirectErrorStream(true);

        try {
          strategyShell = builder.start();

          // To capture output from the shell
          InputStream shellIn = strategyShell.getInputStream();
          IOUtils.copy(shellIn, System.out);

        } catch (Exception e) {
          logger.error(e.getMessage(), e);
        } catch (Error e) {
          logger.error(e.getMessage(), e);
        }
      }
    };

    Executors.newSingleThreadExecutor().execute(run);

  }

  private boolean available(int port) {

    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }

    return false;
  }
}
