package io.tickerstorm.common.data.eventbus;

import java.io.Serializable;

import net.engio.mbassy.listener.IMessageFilter;
import net.engio.mbassy.subscription.SubscriptionContext;

public class BaseFilter implements IMessageFilter<Serializable> {

  public Class<?>[] classFilter = new Class[] {};

  public BaseFilter() {}

  public BaseFilter(Class<?>[] clazzes) {
    this.classFilter = clazzes;
  }

  @Override
  public boolean accepts(Serializable message, SubscriptionContext context) {

    boolean pass = true;

    for (Class<?> c : classFilter) {
      if (!c.isAssignableFrom(message.getClass()))
        pass = false;
    }

    return pass;
  }

}
