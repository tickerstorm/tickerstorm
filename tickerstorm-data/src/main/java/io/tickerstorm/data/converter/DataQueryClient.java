package io.tickerstorm.data.converter;

import java.io.File;
import java.util.List;
import java.util.Map.Entry;

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
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;

import io.tickerstorm.common.data.Locations;
import io.tickerstorm.common.data.converter.DataConverter;
import io.tickerstorm.common.data.converter.DataQuery;
import io.tickerstorm.common.data.converter.Mode;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.common.eventbus.Destinations;

@Service
public class DataQueryClient {

  private static final Logger logger = LoggerFactory.getLogger(DataQueryClient.class);

  @Qualifier(Destinations.HISTORICL_MARKETDATA_BUS)
  @Autowired
  public EventBus historical;

  private CloseableHttpClient client;

  @Autowired
  private List<DataConverter> converters;

  @PostConstruct
  public void init() {

    RequestConfig reqConfig =
        RequestConfig.custom().setSocketTimeout(30 * 1000).setConnectTimeout(3 * 1000).setConnectionRequestTimeout(3 * 1000).build();

    client = HttpClients.custom().setDefaultRequestConfig(reqConfig).build();
  }

  public void query(DataQuery dq) {



    String query = dq.build();

    HttpGet get = new HttpGet(query);
    get.addHeader(HttpHeaders.USER_AGENT,
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36");
    get.addHeader(HttpHeaders.ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
    get.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate, sdch");
    get.addHeader(HttpHeaders.ACCEPT_LANGUAGE, "en-US,en;q=0.8");
    get.addHeader(HttpHeaders.REFERER, "https://www.google.com/");

    if (dq.headers() != null && !dq.headers().isEmpty()) {
      for (Entry<String, String> e : dq.headers().entrySet()) {
        get.addHeader(e.getKey(), e.getValue());
      }
    }

    DataConverter converter = null;
    CloseableHttpResponse response = null;
    try {

      logger.info("Requesting " + query);
      response = client.execute(get);
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {

        if ((response.getFirstHeader(HttpHeaders.CONTENT_TYPE) != null
            && response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue().equalsIgnoreCase("application/zip"))
            || response.getFirstHeader("Content-disposition") != null) {

          response = queryFile(dq, response);

        } else {

          int count = 0;

          for (DataConverter c : converters) {
            if (c.namespaces().contains(dq.namespace())) {
              converter = c;
            }
          }

          if (converter != null && Mode.line.equals(converter.mode())) {

            LineIterator lines = IOUtils.lineIterator(response.getEntity().getContent(), "UTF-8");

            while (lines.hasNext()) {

              String line = (String) lines.next();
              MarketData[] md = converter.convert(line, dq);

              if (md != null && md.length > 0) {
                for (MarketData d : md) {
                  count++;
                  historical.post(d);
                }
              }
            }

          } else if (converter != null && Mode.doc.equals(converter.mode())) {

            String doc = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
            MarketData[] md = converter.convert(doc, dq);

            if (md != null && md.length > 0) {
              for (MarketData d : md) {
                count++;
                historical.post(d);
              }
            }
          }

          logger.info("Converted " + count + " lines from query " + query);
        }
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

  private CloseableHttpResponse queryFile(DataQuery builder, CloseableHttpResponse response) throws Exception {

    String filename = new File(builder.build()).getName();

    logger.info("Downloading file...");
    HttpEntity e = response.getEntity();
    byte[] array = IOUtils.toByteArray(e.getContent());
    Header header = response.getFirstHeader("Content-disposition");

    if (header != null)
      filename = header.getValue().split(";")[1].split("=")[1];

    String path = Locations.FILE_DROP_LOCATION + "/" + builder.provider() + "/" + filename;
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
}
