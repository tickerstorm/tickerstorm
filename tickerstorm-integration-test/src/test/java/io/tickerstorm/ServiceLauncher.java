package io.tickerstorm;

import java.io.InputStream;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Throwables;

public class ServiceLauncher {

  private static Process marketDataShell;
  private static Process strategyShell;

  private static String DATA_SERVICE_JAR =
      "/home/kkarski/.m2/repository/io/tickerstorm/data-service/1.0.0-SNAPSHOT/data-service-1.0.0-SNAPSHOT-exec.jar";
  private static String STRATEGY_SERVICE_JAR =
      "/home/kkarski/.m2/repository/io/tickerstorm/strategy-service/1.0.0-SNAPSHOT/strategy-service-1.0.0-SNAPSHOT-exec.jar";

  public static void launchMarketDataService() {

    Runnable run = new Runnable() {
      @Override
      public void run() {

        ProcessBuilder builder = new ProcessBuilder("/usr/bin/java", "-jar", DATA_SERVICE_JAR);
        builder.redirectErrorStream(true);
        System.out.println(builder.command().toString());

        try {
          marketDataShell = builder.start();

          // To capture output from the shell
          InputStream shellIn = marketDataShell.getInputStream();
          IOUtils.copy(shellIn, System.out);

        } catch (Exception e) {
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
        System.out.println("Exit status" + shellExitStatus);
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
        System.out.println("Exit status" + shellExitStatus);
        strategyShell.getInputStream().close();
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  public static void launchStrategyService() {
    Runnable run = new Runnable() {
      @Override
      public void run() {

        ProcessBuilder builder = new ProcessBuilder("/usr/bin/java", "-jar", STRATEGY_SERVICE_JAR);
        builder.redirectErrorStream(true);
        System.out.println(builder.command().toString());

        try {
          strategyShell = builder.start();

          // To capture output from the shell
          InputStream shellIn = strategyShell.getInputStream();
          IOUtils.copy(shellIn, System.out);

        } catch (Exception e) {
          // TODO: handle exception
        }
      }
    };

    Executors.newSingleThreadExecutor().execute(run);

  }

  public static void main(String args[]) throws Exception {

    launchMarketDataService();
    launchStrategyService();

  }
}
