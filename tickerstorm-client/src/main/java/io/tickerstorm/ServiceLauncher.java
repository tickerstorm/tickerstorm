package io.tickerstorm;

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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class ServiceLauncher {

  private static final Logger logger = LoggerFactory.getLogger(ServiceLauncher.class);

  private static Process marketDataShell;
  private static Process strategyShell;

  private static String DATA_SERVICE_JAR =
      "/home/kkarski/.m2/repository/io/tickerstorm/data-service/1.0.0-SNAPSHOT/data-service-1.0.0-SNAPSHOT-exec.jar";
  private static String STRATEGY_SERVICE_JAR =
      "/home/kkarski/.m2/repository/io/tickerstorm/strategy-service/1.0.0-SNAPSHOT/strategy-service-1.0.0-SNAPSHOT-exec.jar";

  public static void launchMarketDataService(boolean debug, int port, final String monitorPath) {

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
          // TODO: handle exception
        } catch (Error e) {
          // TODO: handle exception
        }
      }
    };

    Executors.newSingleThreadExecutor().execute(run);

  }

  public static void killMarketDataService() {
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

  public static void killStrategyService() {
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

  public static void launchStrategyService(boolean debug, int port) {
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
          // TODO: handle exception
        } catch (Error e) {
          // TODO: handle exception
        }
      }
    };

    Executors.newSingleThreadExecutor().execute(run);

  }

  public static void main(String args[]) throws Exception {

    launchMarketDataService(false, 0, "/tmp/tickerstorm/data-service/monitor");
    launchStrategyService(false, 0);

  }

  private static boolean available(int port) {

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
