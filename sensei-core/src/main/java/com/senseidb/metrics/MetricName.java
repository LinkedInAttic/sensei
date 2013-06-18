package com.senseidb.metrics;


import com.codahale.metrics.MetricRegistry;


/**
 * @author Dmytro Ivchenko
 */
public class MetricName
{
  private final String name;

  public MetricName(Class<?> klass, String name)
  {
    this.name = MetricRegistry.name(klass, name);
  }

  public MetricName(String name, String scope)
  {
    this.name = MetricRegistry.name(scope, name);
  }

  @Override
  public String toString()
  {
    return name;
  }
}
