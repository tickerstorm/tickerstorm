package io.tickerstorm.common.command;

@SuppressWarnings("serial")
public class Trigger extends Command {

  public Trigger(String stream, String type) {
    super(stream, type);
  }

  @Override
  public boolean isValid() {
    return super.validate();
  }

}
