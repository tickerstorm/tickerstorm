package io.tickerstorm.common.data.converter;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.eventbus.Destinations;

public abstract class BaseFileConverter implements FileAlterationListener, FileConverter {

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Autowired
  protected EventBus historical;

  private final static Logger logger = LoggerFactory.getLogger(BaseFileConverter.class);

  @Override
  public void onDirectoryChange(File arg0) {

  }

  @Override
  public void onDirectoryCreate(File arg0) {

  }

  @Override
  public void onDirectoryDelete(File arg0) {

  }

  @Override
  public void onFileChange(File arg0) {

  }

  @Override
  public void onFileCreate(File arg0) {

  }

  @Override
  public void onFileDelete(File arg0) {

  }

  @Override
  public void onStart(FileAlterationObserver arg0) {
    

  }

  @Override
  public void onStop(FileAlterationObserver arg0) {
    

  }

  @Override
  public Mode mode() {
    return Mode.file;
  }

}
