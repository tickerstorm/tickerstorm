package io.tickerstorm.common.entity;

import com.google.common.base.MoreObjects;

@SuppressWarnings("serial")
public class BaseField<T> implements Field<T> {

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((field == null) ? 0 : field.hashCode());
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
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
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

  public BaseField(String field, T value) {
    this.field = field;
    this.value = value;
    this.type = value.getClass();
  }

  public BaseField(String field, Class<?> clazz) {
    this.field = field;
    this.value = null;
    this.type = clazz;
  }

  @Override
  public Class<?> getFieldType() {
    return type;
  }

  public String getName() {
    return field;
  }

  public T getValue() {
    return value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", this.field).add("value", this.value).toString();
  }


}
