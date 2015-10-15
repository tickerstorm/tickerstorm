package io.tickerstorm.common.data.eventbus;

import org.springframework.stereotype.Component;

import com.google.common.base.Throwables;

import net.engio.mbassy.bus.error.IPublicationErrorHandler;
import net.engio.mbassy.bus.error.PublicationError;

@Component
public class MBassadorErrorHandler implements IPublicationErrorHandler {

  public static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MBassadorErrorHandler.class);

  @Override
  public void handleError(PublicationError error) {
    logger.error(error.getMessage(), Throwables.getRootCause(error.getCause()));
    Throwables.propagate(error.getCause());
  }

}
