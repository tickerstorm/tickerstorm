package io.tickerstorm.data;

import io.tickerstorm.entity.MarketData;

import java.io.File;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import net.lingala.zip4j.core.ZipFile;

import com.google.common.base.Throwables;
import com.google.common.io.Files;

@Component
public class ZipFileConverter extends BaseFileConverter {

  private static final Logger logger = LoggerFactory.getLogger(ZipFileConverter.class);

  @Override
  public void onFileCreate(File path) {
    super.onFileCreate(path);

    String extension = Files.getFileExtension(path.getPath());
    String dir = path.getParent() + "/" + UUID.randomUUID().toString();

    if (extension.equalsIgnoreCase("zip")) {

      logger.info("Extacting zip file " + path + " to " + dir);

      try {

        Files.createParentDirs(new File(dir));
        ZipFile zip = new ZipFile(path);
        zip.extractAll(dir);
        path.delete();

      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }

  }

  @Override
  public MarketData[] convert(String line) {
    return null;
  }

  @Override
  public String provider() {
    // TODO Auto-generated method stub
    return null;
  }

}
