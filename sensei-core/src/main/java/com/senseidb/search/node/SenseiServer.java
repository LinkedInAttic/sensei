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
package com.senseidb.search.node;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.metrics.MetricFactory;
import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import javax.management.StandardMBean;

import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;

import proj.zoie.api.DataProvider;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.network.NetworkingException;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.jmx.JmxUtil;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.req.AbstractSenseiRequest;
import com.senseidb.search.req.AbstractSenseiResult;
import com.senseidb.svc.impl.AbstractSenseiCoreService;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;
import com.senseidb.svc.impl.SenseiCoreServiceMessageHandler;
import com.senseidb.svc.impl.SysSenseiCoreServiceImpl;
import com.senseidb.util.NetUtil;
import proj.zoie.api.Zoie;


public class SenseiServer {
  private static final Logger logger = Logger.getLogger(SenseiServer.class);

  private static final String AVAILABLE = "available";
  private static final String UNAVAILABLE = "unavailable";
  private static final String DUMMY_OUT_IP = "74.125.224.0";

  private int _id;
  private int _port;
  private int[] _partitions;
  private NetworkServer _networkServer;
  private ClusterClient _clusterClient;
  private final SenseiCore _core;
  protected volatile Node _serverNode;
  private final List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> _externalSvc;
  private final long _shutdownPauseMillis;

  //private Server _adminServer;

  protected volatile boolean _available = false;

  private final SenseiPluginRegistry pluginRegistry;

  public SenseiServer(int id,
                      int port,
                      int[] partitions,
                      NetworkServer networkServer,
                      ClusterClient clusterClient,
                      SenseiZoieFactory<?> zoieSystemFactory,
                      SenseiIndexingManager indexingManager,
                      SenseiQueryBuilderFactory queryBuilderFactory,
                      List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> externalSvc,
                      SenseiPluginRegistry pluginRegistry,
                      long shutdownPauseMillis)
  {
    this(port,
        networkServer,clusterClient,
        new SenseiCore(id, partitions,zoieSystemFactory, indexingManager, queryBuilderFactory, zoieSystemFactory.getDecorator()),
        externalSvc,
        pluginRegistry,
        shutdownPauseMillis);
  }

  public SenseiServer(int id,
                      int port,
                      int[] partitions,
                      NetworkServer networkServer,
                      ClusterClient clusterClient,
                      SenseiZoieFactory<?> zoieSystemFactory,
                      SenseiIndexingManager indexingManager,
                      SenseiQueryBuilderFactory queryBuilderFactory,
                      List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> externalSvc,
                      SenseiPluginRegistry pluginRegistry)
  {
    this(id, port, partitions, networkServer, clusterClient, zoieSystemFactory, indexingManager,
        queryBuilderFactory, externalSvc, pluginRegistry, 0L);
  }

  public SenseiServer(int port,
                      NetworkServer networkServer,
                      ClusterClient clusterClient,
                      SenseiCore senseiCore,
                      List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> externalSvc,
                      SenseiPluginRegistry pluginRegistry,
                      long shutdownPauseMillis)
  {
    _core = senseiCore;
    this.pluginRegistry = pluginRegistry;
    _id = senseiCore.getNodeId();
    _port = port;
    _partitions = senseiCore.getPartitions();

    _networkServer = networkServer;
    _clusterClient = clusterClient;
    _externalSvc = externalSvc;
    _shutdownPauseMillis = shutdownPauseMillis;
  }


  public SenseiServer(int port,
                      NetworkServer networkServer,
                      ClusterClient clusterClient,
                      SenseiCore senseiCore,
                      List<AbstractSenseiCoreService<AbstractSenseiRequest, AbstractSenseiResult>> externalSvc,
                      SenseiPluginRegistry pluginRegistry)
  {
    this(port, networkServer, clusterClient, senseiCore, externalSvc, pluginRegistry, 0L);
  }

  private static String help(){
    StringBuffer buffer = new StringBuffer();
    buffer.append("Usage: <conf.dir> [availability]\n");
    buffer.append("====================================\n");
    buffer.append("conf.dir - server configuration directory, required\n");
    buffer.append("availability - \"available\" or \"unavailable\", optional default is \"available\"\n");
    buffer.append("====================================\n");
    return buffer.toString();
  }

  public Collection<Zoie<BoboIndexReader, ?>> getZoieSystems()
  {
    return _core.getZoieSystems();
  }

  public int getNumZoieSystems()
  {
    return _core.getNumZoieSystems();
  }

  public void importSnapshot(List<ReadableByteChannel> channels) throws IOException
  {
    _core.importSnapshot(channels);
  }
  public void importSnapshot(List<ReadableByteChannel> channels, long maxBps) throws IOException
  {
    _core.importSnapshot(channels, maxBps);
  }

  public void exportSnapshot(List<WritableByteChannel> channels) throws IOException
  {
    _core.exportSnapshot(channels);
  }

  public void exportSnapshot(List<WritableByteChannel> channels, long maxBps) throws IOException
  {
    _core.exportSnapshot(channels, maxBps);
  }

  public void optimize()
  {
    _core.optimize();
  }

  public DataProvider getDataProvider()
  {
    return _core.getDataProvider();
  }

  public SenseiCore getSenseiCore()
  {
    return _core;
  }

  /*
  public void setAdminServer(Server server)
  {
    _adminServer = server;
  }

  public SenseiNodeInfo getSenseiNodeInfo()
  {
    StringBuffer adminLink = new StringBuffer();
    if (_adminServer != null && _adminServer.getConnectors() != null && _adminServer.getConnectors().length != 0)
    {
      adminLink.append("http://").append(_adminServer.getConnectors()[0].getHost()).append(":")
               .append(_adminServer.getConnectors()[0].getPort());
    }

    return new SenseiNodeInfo(_id, _partitions, _serverNode.getUrl(), adminLink.toString());
  }
  */
  
  public String generateSignature()
  {
      StringBuffer sb = new StringBuffer();
      sb.append(_core.getSystemInfo().getSchema());
      sb.append("-p").append(_core.getNodeId());
      sb.append("-v").append(_core.getSystemInfo().getVersion());
      return sb.toString();
  }

  public void shutdown() {
    // It is important that startup and shutdown be done in the OPPOSITE order
    logger.info("Shutting down the norbert network server...");

    _serverNode = null;
    try {
      _networkServer.shutdown();
    } catch (Throwable throwable) {
      logger.warn("Error shutting down the network server, continuing with shutdown", throwable);
    }

    logger.info("Removing the node from the cluster...");
    try {
      _clusterClient.removeNode(_id);
    } catch (Throwable throwable) {
      logger.warn("Error removing the node from service, continuing with shutdown", throwable);
    }

    logger.info("Shutting down the cluster client...");
    try {
      _clusterClient.shutdown();
    } catch (Throwable throwable) {
      logger.warn("Error shutting down the cluster client, continuing with shutdown", throwable);
    }

    // Clients may take some time to receive an update from zookeeper that the node is still servicing requests.
    // We wait for a preconfigured time to make an effort before shutting down core search internals
    // to not disturb normal service operation
    if(_shutdownPauseMillis > 0) {
      logger.info("Waiting " + _shutdownPauseMillis + " milliseconds for all clients to stop sending requests to server");
      try {
        Thread.sleep(_shutdownPauseMillis);
      } catch (InterruptedException e) {
        logger.warn("Interrupted while waiting, continuing with shutdown ", e);
      }
    }


    logger.info("Shutting down the core search service...");
    try {
      _core.shutdown();
    } catch (Throwable throwable) {
      logger.warn("Error shutting down the core search service, continuing with shutdown", throwable);
    }

    logger.info("Shutting down the plugin registry...");
    try {
      pluginRegistry.stop();
    } catch (Throwable throwable) {
      logger.warn("Error stopping the plugin registry, continuing with shutdown", throwable);
    }

    logger.info("Sensei is shutdown!");
    MetricFactory.stop();
    JmxUtil.unregisterMBeans();
  }

  public void start(boolean available) throws Exception {
    MetricFactory.start();
    _core.start();
//        ClusterClient clusterClient = ClusterClientFactory.newInstance().newZookeeperClient();
    String clusterName = _clusterClient.getServiceName();

    logger.info("Cluster Name: " + clusterName);
    logger.info("Cluster info: " + _clusterClient.toString());

    AbstractSenseiCoreService coreSenseiService = new CoreSenseiServiceImpl(_core);
    AbstractSenseiCoreService sysSenseiCoreService = new SysSenseiCoreServiceImpl(_core);

    // create the zookeeper cluster client
//    SenseiClusterClientImpl senseiClusterClient = new SenseiClusterClientImpl(clusterName, zookeeperURL, zookeeperTimeout, false);
    SenseiCoreServiceMessageHandler senseiMsgHandler =  new SenseiCoreServiceMessageHandler(coreSenseiService);
    SenseiCoreServiceMessageHandler senseiSysMsgHandler =  new SenseiCoreServiceMessageHandler(sysSenseiCoreService);

    _networkServer.registerHandler(senseiMsgHandler, CoreSenseiServiceImpl.PROTO_SERIALIZER);
    _networkServer.registerHandler(senseiSysMsgHandler, SysSenseiCoreServiceImpl.PROTO_SERIALIZER);

    _networkServer.registerHandler(senseiMsgHandler, CoreSenseiServiceImpl.PROTO_V2_SERIALIZER);

    _networkServer.registerHandler(senseiMsgHandler, CoreSenseiServiceImpl.JAVA_SERIALIZER);
    _networkServer.registerHandler(senseiSysMsgHandler, SysSenseiCoreServiceImpl.JAVA_SERIALIZER);

    if (_externalSvc!=null){
      for (AbstractSenseiCoreService svc : _externalSvc){
        _networkServer.registerHandler(new SenseiCoreServiceMessageHandler(svc), svc.getSerializer());
      }
    }
    HashSet<Integer> partition = new HashSet<Integer>();
    for (int partId : _partitions){
      partition.add(partId);
    }

    boolean nodeExists = false;
    try
    {
      logger.info("waiting to connect to cluster...");
      _clusterClient.awaitConnectionUninterruptibly();
      _serverNode = _clusterClient.getNodeWithId(_id);
      nodeExists = (_serverNode != null);
      if (!nodeExists) {
        String ipAddr = getLocalIpAddress();
        logger.info("Node id : " + _id + " IP address : " + ipAddr);
        _serverNode = _clusterClient.addNode(_id, ipAddr, partition);
        logger.info("added node id: " + _id);
      } else
      {
        // node exists

      }
    } catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      throw e;
    }

    try
    {
      logger.info("binding server ...");
      _networkServer.bind(_id, available);

      // exponential backoff
      Thread.sleep(1000);

      _available = available;
      logger.info("started [markAvailable=" + available + "] ...");
      if (nodeExists)
      {
        logger.warn("existing node found, will try to overwrite.");
        try
        {
          // remove node above
          _clusterClient.removeNode(_id);
          _serverNode = null;
        } catch (Exception e)
        {
          logger.error("problem removing old node: " + e.getMessage(), e);
        }
       
        String ipAddr = getLocalIpAddress();
        _serverNode = _clusterClient.addNode(_id, ipAddr, partition);
        Thread.sleep(1000);

        logger.info("added node id: " + _id);
      }
    } catch (NetworkingException e)
    {
      logger.error(e.getMessage(), e);

      try
      {
        if (!nodeExists)
        {
          _clusterClient.removeNode(_id);
          _serverNode = null;
        }
      } catch (Exception ex)
      {
        logger.warn(ex.getMessage());
      } finally
      {
        try
        {
          _networkServer.shutdown();
          _networkServer = null;

        } finally
        {
          _clusterClient.shutdown();
          _clusterClient = null;
        }
      }
      throw e;
    }   
    SenseiServerAdminMBean senseiAdminMBean = getAdminMBean();
    StandardMBean bean = new StandardMBean(senseiAdminMBean, SenseiServerAdminMBean.class);
    JmxUtil.registerMBean(bean, "name", "sensei-server-"+_id);
  }


  private String getLocalIpAddress() throws SocketException,
      UnknownHostException {
    String addr = NetUtil.getHostAddress();
    return String.format("%s:%d", addr, _port);
  }

	private SenseiServerAdminMBean getAdminMBean()
	{
	  return new SenseiServerAdminMBean(){
	  @Override
    public int getId()
      {
        return _id;
      }
      @Override
      public int getPort()
      {
        return _port;
      }
      @Override
      public String getPartitions()
      {
        StringBuffer sb = new StringBuffer();
        if(_partitions.length > 0) sb.append(String.valueOf(_partitions[0]));
        for(int i = 1; i < _partitions.length; i++)
        {
          sb.append(',');
          sb.append(String.valueOf(_partitions[i]));
        }
        return sb.toString();
      }
      @Override
      public boolean isAvailable()
      {
        return SenseiServer.this.isAvailable();
      }
      @Override
      public void setAvailable(boolean available)
      {
        SenseiServer.this.setAvailable(available);
      }
    };
  }

  public void setAvailable(boolean available){
    if (available)
    {
      logger.info("making available node " + _id + " @port:" + _port + " for partitions: " + Arrays.toString(_partitions));
      _networkServer.markAvailable();
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException e)
      {
      }
    } else
    {
      logger.info("making unavailable node " + _id + " @port:" + _port + " for partitions: " + Arrays.toString(_partitions));
      _networkServer.markUnavailable();
    }
    _available = available;
  }

  public boolean isAvailable()
  {
    if (_serverNode != null && _serverNode.isAvailable() == _available)
      return _available;

    try
    {
      Thread.sleep(1000);
      _serverNode = _clusterClient.getNodeWithId(_id);
      if (_serverNode != null && _serverNode.isAvailable() == _available)
        return _available;
    } catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
    _available = (_serverNode != null ? _serverNode.isAvailable() : false);

    return _available;
  }

  /*private static void loadJars(File extDir)
  {
    File[] jarfiles = extDir.listFiles(new FilenameFilter(){
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".jar");
        }
    });

    if (jarfiles!=null && jarfiles.length > 0){
    try{
        URL[] jarURLs = new URL[jarfiles.length];
          ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
          for (int i=0;i<jarfiles.length;++i){
            String jarFile = jarfiles[i].getAbsolutePath();
            logger.info("loading jar: "+jarFile);
            jarURLs[i] = new URL("jar:file://" + jarFile + "!/");
          }
          URLClassLoader classloader = new URLClassLoader(jarURLs,parentLoader);
          logger.info("url classloader: "+classloader);
          Thread.currentThread().setContextClassLoader(classloader);
    }
    catch(MalformedURLException e){
      logger.error("problem loading extension: "+e.getMessage(),e);
    }
  }
}*/

  public  static void main(String[] args) throws Exception {
    if (args.length<1){
      System.out.println(help());
      System.exit(1);
    }

    File confDir = null;

    try {
      confDir = new File(args[0]);
    }
    catch(Exception e) {
      System.out.println(help());
      System.exit(1);
    }

    boolean available = true;
    for(int i = 1; i < args.length; i++)
    {
      if(args[i] != null)
      {
        if(AVAILABLE.equalsIgnoreCase(args[i]))
        {
          available = true;
        }
        if(UNAVAILABLE.equalsIgnoreCase(args[i]))
        {
          available = false;
        }
      }
    }

    /*File extDir = new File(confDir,"ext");

    if (extDir.exists()){
    	logger.info("loading extension jars...");
        loadJars(extDir);
    	logger.info("finished loading extension jars");
    }*/


    SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(confDir, null);

    final SenseiServer server = senseiServerBuilder.buildServer();

    final Server jettyServer = senseiServerBuilder.buildHttpRestServer();

    /*final HttpAdaptor httpAdaptor = senseiServerBuilder.buildJMXAdaptor();


    final ObjectName httpAdaptorName = new ObjectName("mx4j:class=mx4j.tools.adaptor.http.HttpAdaptor,id=1");
	 if (httpAdaptor!=null){
		  try{
			server.mbeanServer.registerMBean(httpAdaptor, httpAdaptorName);
			server.mbeanServer.invoke(httpAdaptorName, "start", null, null);
			httpAdaptor.setProcessor(new XSLTProcessor());
		    logger.info("http adaptor started on port: "+httpAdaptor.getPort());
		  }
		  catch(Exception e){
			logger.error(e.getMessage(),e);
		  }
	  }
*/
    Runtime.getRuntime().addShutdownHook(new Thread(){
      @Override
      public void run(){

        try{
          jettyServer.stop();
        } catch (Exception e) {
          logger.error(e.getMessage(),e);
        }
        finally{
          try{
            server.shutdown();
          }
          finally{
            /*try{
               if (httpAdaptor!=null){
                httpAdaptor.stop();
                server.mbeanServer.invoke(httpAdaptorName, "stop", null, null);
                server.mbeanServer.unregisterMBean(httpAdaptorName);
                logger.info("http adaptor shutdown");
              }
             }
             catch(Exception e){
              logger.error(e.getMessage(),e);
             }*/
          }
        }
      }
    });



    server.start(available);
    jettyServer.start();
  }

}
