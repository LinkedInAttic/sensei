# JRuby client to talk to a Sensei cluster
require 'yaml'
require 'jars/log4j.jar'
require 'jars/fastutil.jar'
require 'jars/lucene-core.jar'
require 'jars/scala-library-2.8.0.jar'
require 'jars/zookeeper-3.2.0.jar'
require 'jars/norbert-cluster_2.8.0-0.5-SNAPSHOT.jar'
require 'jars/norbert-java-cluster_2.8.0-0.5-SNAPSHOT.jar'
require 'jars/norbert-java-network_2.8.0-0.5-SNAPSHOT.jar'
require 'jars/norbert-network_2.8.0-0.5-SNAPSHOT.jar'
require 'jars/sensei-0.0.1.jar'
require 'jars/zoie-2.5.0.jar'
require 'jars/bobo-browse-2.5.0.jar'
require 'jars/protobuf-java.jar'
require 'jars/netty-3.2.0.BETA1.jar'

class SenseiClient

  ZooKeeperClusterClient = com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient
  
  @@logger = org.apache.log4j.Logger.getLogger("Sensei Client")
  
  @clusterClient
  @networkClient
  
  @zkTimeout
  @senseiBroker
  
  def initialize
    self.load_config.each do |k, v|
      instance_variable_set("@#{k}", v)
    end
    @zkTimeout= 30000
    zkurl = #{@zookeeperURL}:#{@zookeeperPort}
    @@logger.info("zookeeper url: #{zkurl}")
    @@logger.info("cluster name: #{@clusterName}")
    @clusterClient = ZooKeeperClusterClient.new(@clusterName,"#{zkurl}",@zkTimeout)
    reqRewriter = com.sensei.search.nodes.impl.NoopRequestScatterRewriter.new
    routerFactory = com.sensei.search.cluster.routing.UniformPartitionedRoutingFactory.new
  
    networkClientConfig = com.linkedin.norbert.javacompat.network.NetworkClientConfig.new
    networkClientConfig.setServiceName(@clusterName)
    networkClientConfig.setZooKeeperConnectString(zkurl)
    networkClientConfig.setZooKeeperSessionTimeoutMillis(@zkTimeout)
    networkClientConfig.setConnectTimeoutMillis(1000)
    networkClientConfig.setWriteTimeoutMillis(150)
    networkClientConfig.setMaxConnectionsPerNode(5)
    networkClientConfig.setStaleRequestTimeoutMins(10)
    networkClientConfig.setStaleRequestCleanupFrequencyMins(10)
    networkClientConfig.setClusterClient(@clusterClient)
    @networkClient = com.sensei.search.cluster.client.SenseiNetworkClient.new(networkClientConfig,routerFactory)
    @senseiBroker =com.sensei.search.nodes.SenseiBroker.new(@networkClient, @clusterClient, reqRewriter, routerFactory)
    
    @clusterClient.awaitConnectionUninterruptibly
    @@logger.info("connected to cluster...")
  end

  # loads the yml config file which tells where the backend resides, etc.
  def load_config
    begin
      @@stored_config ||= YAML.load(File.open(File.expand_path("#{File.dirname(__FILE__)}/../conf/sensei-client.yml")))
      return @@stored_config
    rescue
      raise "Could not find a conf/sensei-client.yml file"
    end
  end
  
  def search(params)
    req = com.sensei.search.req.SenseiRequest.new
    offset = Integer(params[:offset])
    count = Integer(params[:count])
    q = params[:q]
    if not q.nil? then
      senseiQuery = com.sensei.search.req.StringQuery.new(q)
      req.setQuery(senseiQuery)
    end
    req.setOffset(offset)
    req.setCount(count)
    
    senseiResult = @senseiBroker.browse(req)
    result = {'totalhits' => senseiResult.getNumHits,'offset' => offset,'count' => count}.to_json
    return result
  end
  
  def shutdown
    @clusterClient.shutdown
  end
  
end
