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

package com.senseidb.federated.broker;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.jetty.Server;

import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.federated.broker.proxy.SenseiBrokerProxy;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public class SingleNodeStarter {
  private static boolean serverStarted = false;
  private static Server jettyServer;
  private static SenseiServer server;

  public static void start(String localPath, int expectedDocs) {
    start(new File(getUri(localPath)), expectedDocs);
  }

  public static void start(File confDir, int expectedDocs) {
    if (!serverStarted) {
      try {
        PropertiesConfiguration senseiConfiguration = new PropertiesConfiguration(new File(confDir, "sensei.properties"));
        final String indexDir = senseiConfiguration.getString(SenseiConfParams.SENSEI_INDEX_DIR);
        rmrf(new File(indexDir));
        SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(confDir, null);
        server = senseiServerBuilder.buildServer();
        jettyServer = senseiServerBuilder.buildHttpRestServer();
        server.start(true);
        jettyServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              jettyServer.stop();
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              server.shutdown();
              rmrf(new File(indexDir));
            }
          }
        });
        SenseiBrokerProxy brokerProxy = SenseiBrokerProxy.valueOf(senseiConfiguration, new HashMap<String, String>());
        while (true) {
          SenseiResult senseiResult = brokerProxy.doQuery(new SenseiRequest()).get(0);
          int totalDocs = senseiResult.getTotalDocs();
          System.out.println("TotalDocs = " + totalDocs);
          if (totalDocs >= expectedDocs) {
            break;
          }
          Thread.sleep(100);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static boolean rmrf(File f) {
    if (f == null || !f.exists()) {
      return true;
    }
    if (f.isDirectory()) {
      for (File sub : f.listFiles()) {
        if (!rmrf(sub))
          return false;
      }
    }
    return f.delete();
  }

  private static URI getUri(String localPath) {
    try {
      return FederatedBrokerIntegrationTest.class.getClassLoader().getResource(localPath).toURI();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }
}
