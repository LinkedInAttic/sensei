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

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Factory class for creating metric instances. It replaces the {@link com.codahale.metrics.Metric} class
 * so that the {@link com.codahale.metrics.MetricRegistry} used can be start and stop.
 */
public final class MetricFactory {

  private static final AtomicReference<MetricFactory> FACTORY = new AtomicReference<MetricFactory>();

  private static final int DEFAULT_SIZE = 1028;
  private static final double DEFAULT_ALPHA = 1;

  private final MetricRegistry _registry;
  private final JmxReporter _reporter;

  private final ConcurrentMap<String, Metric> _metrics;

  /**
   * Stops the current MetricFactory. It will stop all threads owned by the factory and unregister all mbeans
   * created through the factory.
   */
  public static void stop() {
    MetricFactory oldFactory = FACTORY.getAndSet(null);
    if (oldFactory != null) {
      oldFactory._reporter.stop();
    }
  }

  public static Timer newTimer(MetricName metricName) {
    Timer timer = new Timer(new ExponentiallyDecayingReservoir(DEFAULT_SIZE, DEFAULT_ALPHA));
    return getInstance().register(metricName, timer);
  }

  public static Meter newMeter(MetricName metricName) {
    Meter meter = new Meter();
    return getInstance().register(metricName, meter);
  }

  public static Counter newCounter(MetricName metricName) {
    Counter counter = new Counter();
    return getInstance().register(metricName, counter);
  }

  public static Histogram newHistogram(MetricName metricName) {
    Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir(DEFAULT_SIZE, DEFAULT_ALPHA));
    return getInstance().register(metricName, histogram);
  }

  private <M extends Metric> M register(MetricName metricName, M metric)
  {
    String name = metricName.toString();
    M existing = (M)_metrics.get(name);
    if (existing != null) {
      return existing;
    } else {
      M prev = (M)_metrics.putIfAbsent(name, metric);
      if (prev == null) {
        _registry.register(name, metric);
        return metric;
      } else {
        return prev;
      }
    }
  }

  /**
   * Returns a {@link MetricFactory}. It will start this factory if it is not started.
   */
  private static MetricFactory getInstance() {
    MetricFactory factory = FACTORY.get();
    while (factory == null) {
      factory = new MetricFactory();
      if (FACTORY.compareAndSet(null, factory)) {
        factory._reporter.start();
      } else {
        factory = FACTORY.get();
      }
    }
    return factory;
  }

  private MetricFactory() {
    _registry = new MetricRegistry();
    _reporter = JmxReporter.forRegistry(_registry).inDomain(MetricsConstants.Domain).build();
    _metrics = new ConcurrentHashMap<String, Metric>();
  }
}
