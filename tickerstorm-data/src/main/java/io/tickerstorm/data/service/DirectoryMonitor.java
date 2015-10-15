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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.tickerstorm.common.data.converter.BaseFileConverter;

@Service
public class DirectoryMonitor {

  @Autowired
  private List<BaseFileConverter> listeners = new ArrayList<BaseFileConverter>();

  private FileAlterationMonitor monitor;

  @PostConstruct
  public void init() throws Exception {

    final File directory = new File("./data");
    FileAlterationObserver fao = new FileAlterationObserver(directory);

    for (BaseFileConverter l : listeners) {
      fao.addListener(l);

      if (!StringUtils.isEmpty(l.provider()))
        FileUtils.forceMkdir(new File("./data/" + l.provider()));
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
