package io.tickerstorm.data.service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.tickerstorm.common.data.converter.BaseFileConverter;

@Service
public class DirectoryMonitor {

  public final static Logger logger = LoggerFactory.getLogger(DirectoryMonitor.class);

  @Autowired
  private List<BaseFileConverter> listeners = new ArrayList<BaseFileConverter>();

  private FileAlterationMonitor monitor;

  private static String path = System.getenv("service.data.monitor.location");

  @PostConstruct
  public void init() throws Exception {

    if (StringUtils.isEmpty(path)) {
      path = "./data";
    }

    final File directory = new File(path);
    FileAlterationObserver fao = new FileAlterationObserver(directory);

    logger.info("Monitoring location: " + directory.getAbsolutePath());

    for (BaseFileConverter l : listeners) {
      fao.addListener(l);

      if (!StringUtils.isEmpty(l.provider()))
        FileUtils.forceMkdir(new File(path + l.provider()));
    }

    monitor = new FileAlterationMonitor(2000);
    monitor.addObserver(fao);

    System.out.println("Starting monitor. CTRL+C to stop.");
    monitor.start();

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          System.out.println("Stopping monitor.");
          monitor.stop();
        } catch (Exception ignored) {
        }
      }
    }));

  }

  @PreDestroy
  public void destroy() throws Exception {
    if (monitor != null)
      monitor.stop();
  }

}
