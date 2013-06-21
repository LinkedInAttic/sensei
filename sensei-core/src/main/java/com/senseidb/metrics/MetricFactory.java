/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.metrics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.JmxReporter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Factory class for creating metric instances. It replaces the {@link com.yammer.metrics.Metrics} class
 * so that the {@link com.yammer.metrics.core.MetricsRegistry} used can be start and stop.
 */
public final class MetricFactory {

  private static final AtomicReference<MetricFactory> FACTORY = new AtomicReference<MetricFactory>();

  private final MetricsRegistry _registry;
  private final JmxReporter _reporter;

  /**
   * Starts a new MetricFactory. If there exists an old MetricFactory, it will be stopped.
   */
  public static void start() {
    MetricFactory oldFactory = FACTORY.getAndSet(new MetricFactory().startAll());
    if (oldFactory != null) {
      oldFactory.stopAll();
    }
  }

  /**
   * Stops the current MetricFactory. It will stop all threads owned by the factory and unregister all mbeans
   * created through the factory.
   */
  public static void stop() {
    MetricFactory oldFactory = FACTORY.getAndSet(null);
    if (oldFactory != null) {
      oldFactory.stopAll();
    }
  }

  /**
   * @see MetricsRegistry#newTimer(com.yammer.metrics.core.MetricName, java.util.concurrent.TimeUnit, java.util.concurrent.TimeUnit)
   */
  public static Timer newTimer(MetricName metricName,
                        TimeUnit durationUnit,
                        TimeUnit rateUnit) {
    return getRegistry().newTimer(metricName, durationUnit, rateUnit);
  }

  /**
   * @see MetricsRegistry#newMeter(com.yammer.metrics.core.MetricName, String, java.util.concurrent.TimeUnit)
   */
  public static Meter newMeter(MetricName metricName,
                        String eventType,
                        TimeUnit unit) {
    return getRegistry().newMeter(metricName, eventType, unit);
  }

  /**
   * @see MetricsRegistry#newCounter(com.yammer.metrics.core.MetricName)
   */
  public static Counter newCounter(MetricName metricName) {
    return getRegistry().newCounter(metricName);
  }

  /**
   * @see MetricsRegistry#newHistogram(com.yammer.metrics.core.MetricName, boolean)
   */
  public static Histogram newHistogram(MetricName metricName, boolean biased) {
    return getRegistry().newHistogram(metricName, biased);
  }

  /**
   * Returns a {@link MetricsRegistry}. It will start this factory if it is not started.
   */
  private static MetricsRegistry getRegistry() {
    MetricFactory factory = FACTORY.get();
    while (factory == null) {
      start();
      factory = FACTORY.get();
    }
    return factory._registry;
  }

  private MetricFactory() {
    _registry = new MetricsRegistry();
    _reporter = new JmxReporter(_registry);
  }

  /**
   * Starts the {@link JmxReporter}.
   *
   * @return this instance.
   */
  private MetricFactory startAll() {
    _reporter.start();
    return this;
  }

  /**
   * Stops all threads owned by the {@link MetricsRegistry} and unregister
   * all mbeans owned by {@link JmxReporter}.
   */
  private void stopAll() {
    _registry.shutdown();
    _reporter.shutdown();
  }
}
