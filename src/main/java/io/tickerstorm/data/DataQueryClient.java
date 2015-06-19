package io.tickerstorm.data;

import io.tickerstorm.entity.MarketData;

import java.io.File;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;

@Service
public class DataQueryClient {

  @Qualifier("historical")
  @Autowired
  public EventBus bus;

  private static final Logger logger = LoggerFactory.getLogger(DataQueryClient.class);

  private CloseableHttpClient client;

  @PostConstruct
  public void init() {

    RequestConfig reqConfig = RequestConfig.custom().setSocketTimeout(30 * 1000).setConnectTimeout(3 * 1000)
        .setConnectionRequestTimeout(3 * 1000).build();

    client = HttpClients.custom().setDefaultRequestConfig(reqConfig).build();
  }

  private CloseableHttpResponse queryFile(QueryBuilder builder, CloseableHttpResponse response) throws Exception {

    logger.info("Downloading file...");
    byte[] array = IOUtils.toByteArray(response.getEntity().getContent());
    Header header = response.getFirstHeader("Content-disposition");
    String filename = null;

    if (header != null)
      filename = header.getValue().split(";")[1].split("=")[1];

    String path = "./data/" + builder.provider() + "/" + filename;
    logger.info("Writing file to " + path);

    File f = new File(path);
    Files.createParentDirs(f);

    if (f.exists()){
      logger.info("Deleting exisitng file " + f);
      f.delete();
    }

    Files.write(array, f);
    Thread.sleep(50); //give it some time to flush to disk

    return response;

  }

  public void query(QueryBuilder builder) {

    String query = builder.build();

    HttpGet get = new HttpGet(query);

    CloseableHttpResponse response = null;
    try {
      logger.info("Requesting " + query);
      response = client.execute(get);

      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {

        if (response.getFirstHeader("Content-disposition") != null) {

          response = queryFile(builder, response);

        } else {

          LineIterator lines = IOUtils.lineIterator(response.getEntity().getContent(), "UTF-8");

          while (lines.hasNext()) {

            String line = (String) lines.next();
            MarketData[] md = builder.converter().convert(line);

            if (md != null) {
              for (MarketData d : md) {
                bus.post(d);
              }
            }
          }
        }
      }

    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      EntityUtils.consumeQuietly(response.getEntity());

      try {
        response.close();
      } catch (Exception ex) {
        // nothing
      }
    }

  }
}
