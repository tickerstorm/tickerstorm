package io.tickerstorm.common.entity;

import com.google.common.base.MoreObjects;

@SuppressWarnings("serial")
public class BaseField<T> implements Field<T> {

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((eventId == null) ? 0 : eventId.hashCode());
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + ((stream == null) ? 0 : stream.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BaseField other = (BaseField) obj;
    if (eventId == null) {
      if (other.eventId != null)
        return false;
    } else if (!eventId.equals(other.eventId))
      return false;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
      return false;
    if (stream == null) {
      if (other.stream != null)
        return false;
    } else if (!stream.equals(other.stream))
      return false;
    if (type == null) {
      if (other.type != null)
        return false;
    } else if (!type.equals(other.type))
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

  public final String field;
  public final T value;
  public final Class<?> type;
  public final String eventId;
  public final String stream;

  /**
   * Convenience constructor when extrapolating a new field based on an existing field of the same
   * type
   * 
   * @param f
   * @param appendName
   * @param value
   */
  public BaseField(Field<?> f, String appendName, T value) {
    this.field = f.getName() + "|" + appendName;
    this.value = value;
    this.type = value.getClass();
    this.eventId = f.getEventId();
    this.stream = f.getStream();
  }


  /**
   * Convenience constructor when extrapolating a new field based on an existing field of the same
   * type but will a null value
   * 
   * @param f
   * @param appendName
   * @param value
   */
  public BaseField(Field<?> f, String appendName, Class<?> clazz) {
    this.field = f.getName() + "|" + appendName;
    this.value = null;
    this.type = clazz;
    this.eventId = f.getEventId();
    this.stream = f.getStream();
  }

  /**
   * New field constructor
   * 
   * @param eventId
   * @param field
   * @param value
   */
  public BaseField(String eventId, String field, T value) {
    this.field = field;

    if (value == null)
      throw new IllegalArgumentException("Value must not be null");

    this.value = value;
    this.type = value.getClass();
    this.eventId = eventId;
    this.stream = MarketData.parseStream(eventId);
  }

  protected BaseField(String eventId, String field, T value, Class<?> clazz) {
    this.field = field;
    this.value = value;

    if (value == null)
      this.type = clazz;
    else
      this.type = value.getClass();

    this.eventId = eventId;
    this.stream = MarketData.parseStream(eventId);
  }

  /**
   * New field constructor
   * 
   * @param eventId
   * @param field
   * @param clazz
   */
  public BaseField(String eventId, String field, Class<?> clazz) {
    this.field = field;
    this.value = null;
    this.type = clazz;
    this.eventId = eventId;
    this.stream = MarketData.parseStream(eventId);
  }

  @Override
  public Class<?> getFieldType() {
    return type;
  }

  @Override
  public String getStream() {
    return stream;
  }


  public String getName() {
    return field;
  }

  public T getValue() {
    return value;
  }

  @Override
  public int compareTo(Field<T> o) {
    return MarketData.parseTimestamp(o.getEventId()).compareTo(MarketData.parseTimestamp(this.getEventId()));
  }
  
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("stream", this.stream).add("name", this.field).add("value", this.value).add("class", this.type).add("eventId", eventId)
        .toString();
  }

  public String getEventId() {
    return eventId;
  }


}
