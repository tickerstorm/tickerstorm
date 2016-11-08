package io.tickerstorm.common.config;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class Node<T> {
  private Set<Node<T>> children = new HashSet<Node<T>>();
  private Node<T> parent = null;
  private T data = null;
  private boolean strict = true;

  public Node(T data) {
    this.data = data;
  }

  public Node(T data, boolean strict) {
    this.data = data;
    this.strict = strict;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
  
  public Node(T data, Node<T> parent) {
    this.data = data;
    this.setParent(parent);
  }

  public Set<Node<T>> getChildren() {
    return children;
  }

  public Set<T> getChildrensData() {

    Set<Node<T>> ns = getChildren();
    return ns.stream().map(c -> c.data).collect(Collectors.toSet());

  }

  public boolean hasChild(T name) {

    if (!strict && String.class.isAssignableFrom(name.getClass()))
      return this.children.stream().filter(c -> ((String) c.data).equalsIgnoreCase((String) name)).count() > 0;

    return this.children.stream().filter(c -> c.data.equals(name)).count() > 0;
  }

  public Node<T> getChild(T name) {

    if (!strict && String.class.isAssignableFrom(name.getClass()))
      return this.children.stream().filter(c -> ((String) c.data).equalsIgnoreCase((String) name)).collect(Collectors.toList()).get(0);

    return this.children.stream().filter(c -> c.data.equals(name)).collect(Collectors.toList()).get(0);
  }

  public Node<T> getOtherwiseChild(T name, T otherwise) {

    if (!hasChild(name))
      return getChild(otherwise);

    return getChild(name);
  }



  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
    result = prime * result + ((parent == null) ? 0 : parent.hashCode());
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
    Node other = (Node) obj;
    if (data == null) {
      if (other.data != null)
        return false;
    } else if (!data.equals(other.data))
      return false;
    if (parent == null) {
      if (other.parent != null)
        return false;
    } else if (!parent.equals(other.parent))
      return false;
    return true;
  }

  public void setParent(Node<T> parent) {
    
    if(this.parent == null || !this.parent.equals(parent)){
    parent.children.add(this);
    this.strict = parent.strict;
    this.parent = parent;
    }
  }

  public void addChild(T data) {
    Node<T> child = new Node<T>(data);
    addChild(child);
  }

  public void addChildren(Collection<T> children) {
    for (T n : children) {
      addChild(n);
    }
  }

  public void addChild(Node<T> child) {

    if (!this.children.contains(child)) {
      child.setParent(this);
      child.strict = this.strict;
      this.children.add(child);
    }
  }

  public T getData() {
    return this.data;
  }

  public void setData(T data) {
    this.data = data;
  }

  public boolean isRoot() {
    return (this.parent == null);
  }

  public boolean isLeaf() {
    if (this.children.size() == 0)
      return true;
    else
      return false;
  }

  public boolean isStrict() {
    return this.strict;
  }

  public void removeParent() {
    this.parent = null;
  }
}
