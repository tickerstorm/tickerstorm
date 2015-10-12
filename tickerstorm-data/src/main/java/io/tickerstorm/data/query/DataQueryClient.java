package io.tickerstorm.data.query;

import java.io.File;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
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
import com.google.common.io.Files;

import io.tickerstorm.data.converter.DataConverter.Mode;
import io.tickerstorm.entity.MarketData;
import net.engio.mbassy.bus.MBassador;

@Service
public class DataQueryClient {

  @Qualifier("historical")
  @Autowired
  public MBassador<MarketData> historical;

  private static final Logger logger = LoggerFactory.getLogger(DataQueryClient.class);

  private CloseableHttpClient client;

  @PostConstruct
  public void init() {

    RequestConfig reqConfig = RequestConfig.custom().setSocketTimeout(30 * 1000)
        .setConnectTimeout(3 * 1000).setConnectionRequestTimeout(3 * 1000).build();

    client = HttpClients.custom().setDefaultRequestConfig(reqConfig).build();
  }

  private CloseableHttpResponse queryFile(QueryBuilder builder, CloseableHttpResponse response)
      throws Exception {

    String filename = new File(builder.build()).getName();

    logger.info("Downloading file...");
    HttpEntity e = response.getEntity();
    byte[] array = IOUtils.toByteArray(e.getContent());
    Header header = response.getFirstHeader("Content-disposition");

    if (header != null)
      filename = header.getValue().split(";")[1].split("=")[1];

    String path = "./data/" + builder.provider() + "/" + filename;
    logger.info("Writing file to " + path);

    File f = new File(path);
    Files.createParentDirs(f);

    if (f.exists()) {
      logger.info("Deleting exisitng file " + f);
      f.delete();
    }

    Files.write(array, f);
    Thread.sleep(50); // give it some time to flush to disk

    return response;

  }

  public void query(QueryBuilder builder) {

    String query = builder.build();

    HttpGet get = new HttpGet(query);
    get.addHeader(HttpHeaders.USER_AGENT,
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36");
    get.addHeader(HttpHeaders.ACCEPT,
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
    get.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate, sdch");
    get.addHeader(HttpHeaders.ACCEPT_LANGUAGE, "en-US,en;q=0.8");
    get.addHeader(HttpHeaders.REFERER, "https://www.google.com/");

    CloseableHttpResponse response = null;
    try {
      logger.info("Requesting " + query);
      response = client.execute(get);
      int count = 0;
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {

        if ((response.getFirstHeader(HttpHeaders.CONTENT_TYPE) != null
            && response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue()
                .equalsIgnoreCase("application/zip"))
            || response.getFirstHeader("Content-disposition") != null) {

          response = queryFile(builder, response);

        } else if (Mode.line.equals(builder.converter().mode())) {

          LineIterator lines = IOUtils.lineIterator(response.getEntity().getContent(), "UTF-8");

          while (lines.hasNext()) {

            String line = (String) lines.next();
            MarketData[] md = builder.converter().convert(line);

            if (md != null && md.length > 0) {
              for (MarketData d : md) {
                count++;
                historical.publish(d);
              }
            }
          }

        } else if (Mode.doc.equals(builder.converter().mode())) {

          String doc = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
          MarketData[] md = builder.converter().convert(doc);

          if (md != null && md.length > 0) {
            for (MarketData d : md) {
              count++;
              historical.publish(d);
            }
          }
        }

        logger.info("Converted " + count + " lines from query " + query);
      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
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