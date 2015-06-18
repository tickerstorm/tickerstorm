package io.tickerstorm.data;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseFileConverter implements FileAlterationListener, DataConverter {

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
    // TODO Auto-generated method stub

  }

  @Override
  public void onStop(FileAlterationObserver arg0) {
    // TODO Auto-generated method stub

  }

}
