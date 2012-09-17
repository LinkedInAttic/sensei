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
package com.senseidb.search.node.impl;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;

public class CompositeDataConsumer<T> implements DataConsumer<T> {

  private List<DataConsumer<T>> _consumerList;
  private Comparator<String> _versionComparator;
  public CompositeDataConsumer(Comparator<String> versionComparator){
    _consumerList = new LinkedList<DataConsumer<T>>();
    _versionComparator = versionComparator;
  }
  
  public void addDataConsumer(DataConsumer<T> dataConsumer){
    _consumerList.add(dataConsumer);
  }
  
  @Override
  public void consume(Collection<DataEvent<T>> events)
      throws ZoieException {
    for (DataConsumer<T> consumer : _consumerList){
      consumer.consume(events);
    }
  }

  @Override
  public String getVersion() {
    String version = null;
    if (_consumerList!=null){
      for (DataConsumer<T> consumer : _consumerList){
        String ver = consumer.getVersion();
        if (_versionComparator.compare(ver, version)<0){
          version = ver;
        }
      }
    }
    return version;
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
}
