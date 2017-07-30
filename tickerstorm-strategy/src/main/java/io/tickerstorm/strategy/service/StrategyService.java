/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.strategy.service;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.tickerstorm.common.cache.CacheManager;
import io.tickerstorm.common.command.Command;
import io.tickerstorm.common.command.Markers;
import io.tickerstorm.common.config.TransformerConfig;
import io.tickerstorm.common.eventbus.Destinations;
import io.tickerstorm.common.reactive.Observations.Session;
import io.tickerstorm.strategy.processor.BaseProcessor;
import io.tickerstorm.common.reactive.Notification;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Created by kkarski on 5/29/17.
 */
@Service
public class StrategyService {

  private final static Map<String, Map<String, String>> processorStatus = new HashMap<>();

  @Qualifier(Destinations.NOTIFICATIONS_BUS)
  @Autowired
  protected EventBus notificationsBus;
  @Autowired
  @Qualifier(Destinations.COMMANDS_BUS)
  private EventBus commandsBus;

  @PostConstruct
  public void init() throws Exception {
    commandsBus.register(this);
    notificationsBus.register(this);
  }

  @PreDestroy
  public void destroy() throws Exception {
    commandsBus.unregister(this);
    notificationsBus.unregister(this);
  }

  @Subscribe
  public void onCommand(Command command) throws Exception {

    if (command.config != null && command.config.containsKey(BaseProcessor.TRANSFORMERS_YML_NODE)) {

      Map<String, List<Map<String, String>>> trans = (Map<String, List<Map<String, String>>>) command.config.get(BaseProcessor.TRANSFORMERS_YML_NODE);

      if (trans.containsKey(name())) {
        TransformerConfig config = new TransformerConfig(trans.get(name()));
        BaseProcessor.getConfig(command.getStream()).put(BaseProcessor.TRANSFORMER_CONFIG_KEY, config);
      }

      // Initialize cache
      CacheManager.getInstance(command.getStream());
    } else {
      getConfig(command.getStream()).put(BaseProcessor.TRANSFORMER_CONFIG_KEY, new TransformerConfig(false));
    }

    Notification not = new Notification(command);
    not.markers.add(Markers.SUCCESS.toString());
    not.properties.put("transformer", name());
    notificationsBus.post(not);


  }

  @Subscribe
  public void onNotification(Notification not) throws Exception {

    if(Session.started.test(not) && not.getMarkers().contains("transformer")){

    }

  }
}
