package io.tickerstorm.common.entity;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.math.BigDecimal;

@SuppressWarnings("serial")
public class BaseField<T> implements Field<T> {

  public final String field;
  public final T value;
  public final Class<?> type;
  public final String eventId;

  /**
   * Convenience constructor when extrapolating a new field based on an existing field of the same
   * type but will a null value
   */
  public BaseField(Field<?> f, String appendName, Class<?> clazz) {
    this.field = f.getName() + "|" + appendName;
    this.value = null;
    this.type = clazz;
    this.eventId = f.getEventId();
  }

  /**
   * Convenience constructor when extrapolating a new field based on an existing field of the same
   * type
   */
  public BaseField(Field<?> f, String appendName, T value) {
    this.field = f.getName() + "|" + appendName;
    this.value = value;
    this.type = value.getClass();
    this.eventId = f.getEventId();
  }

  /**
   * New field constructor
   */
  public BaseField(String eventId, String field, Class<?> clazz) {
    this.field = field;
    this.value = null;
    this.type = clazz;
    this.eventId = eventId;
  }

  /**
   * New field constructor
   */
  public BaseField(String eventId, String field, T value) {
    this.field = field;

    if (value == null) {
      throw new IllegalArgumentException("Value must not be null");
    }

    this.value = value;
    this.type = value.getClass();
    this.eventId = eventId;

  }

  protected BaseField(String eventId, String field, T value, Class<?> clazz) {
    this.field = field;
    this.value = value;

    if (value == null) {
      this.type = clazz;
    } else {
      this.type = value.getClass();
    }

    this.eventId = eventId;

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BaseField)) {
      return false;
    }
    BaseField<?> baseField = (BaseField<?>) o;
    return Objects.equal(field, baseField.field) &&
        Objects.equal(getValue(), baseField.getValue()) &&
        Objects.equal(type, baseField.type) &&
        Objects.equal(getEventId(), baseField.getEventId());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(field, getValue(), type, getEventId());
  }

  @Override
  public int compareTo(Field<T> o) {
    return MarketData.parseTimestamp(o.getEventId()).compareTo(MarketData.parseTimestamp(this.getEventId()));
  }

  public String getEventId() {
    return eventId;
  }

  @Override
  public Class<?> getFieldType() {
    return type;
  }

  public String getName() {
    return field;
  }

  public T getValue() {

    if (getFieldType().equals(BigDecimal.class)) {
      return (T) ((BigDecimal) value).setScale(4, BigDecimal.ROUND_HALF_DOWN);
    }

    return value;
  }


  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("stream", this.getStream()).add("name", this.field).add("value", getValue())
        .add("class", this.type).add("eventId", eventId).toString();
  }


}
