package io.tickerstorm.data.converter;

import io.tickerstorm.entity.MarketData;

import java.io.File;
import java.util.UUID;

import net.lingala.zip4j.core.ZipFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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

      } catch (Exception e) {

        logger.error(e.getMessage(), e);

      } finally {
        path.delete();
      }
    }

  }

  @Override
  public MarketData[] convert(String line) {
    return null;
  }

  @Override
  public String provider() {
    return null;
  }

}
