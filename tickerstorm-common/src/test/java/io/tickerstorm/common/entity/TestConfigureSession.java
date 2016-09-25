package io.tickerstorm.common.entity;

import java.io.InputStream;

import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.DefaultResourceLoader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;

import io.tickerstorm.common.data.eventbus.Destinations;

public class TestConfigureSession {

  private Session session;

  @InjectMocks
  private SessionFactory factory;

  @Qualifier(Destinations.COMMANDS_BUS)
  @Mock
  private EventBus eventBus = new EventBus();

  @BeforeMethod
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testConfigureViaYaml() throws Exception {

    InputStream content = new DefaultResourceLoader().getResource("classpath:yml/sample.yml").getInputStream();
    session = factory.newSession();
    session.configure(content);
    Assert.assertNotNull(session.config);
    Assert.assertTrue(session.config.size() > 0);

  }

}
