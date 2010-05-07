package com.sensei.search.cluster.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.search.SortField;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.cluster.javaapi.Node;
import com.linkedin.norbert.cluster.javaapi.ZooKeeperClusterClient;
import com.linkedin.norbert.network.javaapi.PartitionedNetworkClient;
import com.sensei.search.cluster.routing.UniformPartitionedRoutingFactory;
import com.sensei.search.nodes.SenseiBroker;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.util.SenseiDefaults;

public class SenseiClusterClient {

	static SenseiBroker _broker = null;
	
	static BrowseRequestBuilder _reqBuilder = new BrowseRequestBuilder();
	
	private static final String DEFAULT_CLUSTER_NAME = "sensei";
	private static final String DEFAULT_ZK_URL = "localhost:2181";
	
	private static PartitionedNetworkClient<Integer> _networkClient = null;
  
	private static ClusterClient _cluster = null;
	
	private static void shutdown() throws NorbertException{
		try{
		   System.out.println("shutting down client...");
		  _broker.shutdown();
		}
		finally{
          System.out.println("shutting down cluster...");

          try{
        	  _cluster.shutdown();
          }
          finally{
          }
          
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{

	    File confFile = null;
	    ApplicationContext springCtx = null;
	    if (args.length < 1){
          System.out.println("no config specified, defaulting to default: " + DEFAULT_CLUSTER_NAME + " at " + DEFAULT_ZK_URL);
	    }
	    else{
	      File confDir = new File(args[0]);
	      confFile = new File(confDir , SenseiDefaults.SENSEI_NETWORK_CONF_FILE);
	      springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
	    }

	    // create the network client
	    _networkClient = (SenseiNetworkClient)springCtx.getBean("network-client");
        _cluster = (ZooKeeperClusterClient)springCtx.getBean("zookeeper-cluster-client");
        
	     System.out.println("connecting to cluster at "+ DEFAULT_ZK_URL + "... ");

	    // register the request-response messages
        _networkClient.registerRequest(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance());
        
        // create the broker
		_broker = new SenseiBroker(_networkClient, null, new UniformPartitionedRoutingFactory());
	    _cluster.addListener(_broker);

		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				try {
					shutdown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		BufferedReader cmdLineReader = new BufferedReader(new InputStreamReader(System.in));
		while(true){
			try{ 
				_cluster.awaitConnectionUninterruptibly();
				System.out.println("connected to cluster...");
				System.out.print("> ");
				String line = cmdLineReader.readLine();
				while(true){
					try{
					  processCommand(line, _cluster);
					}
					catch(NorbertException ne){
						ne.printStackTrace();
					}
					System.out.print("> ");
					line = cmdLineReader.readLine();
				}
				
			}
			catch(InterruptedException ie){
				throw new Exception(ie.getMessage(),ie);
			}
		}
	}
	
	static void processCommand(String line, ClusterClient cluster) throws NorbertException, InterruptedException, ExecutionException{
		if (line == null || line.length() == 0) return;
		String[] parsed = line.split(" ");
		if (parsed.length == 0) return;
		
		String cmd = parsed[0];
		
		String[] args = new String[parsed.length -1 ];
		if (args.length > 0){
			System.arraycopy(parsed, 1, args, 0, args.length);
		}
		
		if ("exit".equalsIgnoreCase(cmd)){
			System.exit(0);
		}
		else if ("help".equalsIgnoreCase(cmd)){
			System.out.println("help - prints this message");
			System.out.println("exit - quits");
			System.out.println("nodes - prints a list of node information");
			System.out.println("query <query string> - sets query text");
			System.out.println("facetspec <name>:<minHitCount>:<maxCount>:<sort> - add facet spec");
			System.out.println("page <offset>:<count> - set paging parameters");
			System.out.println("select <name>:<value1>,<value2>... - add selection, with ! in front of value indicates a not");
			System.out.println("sort <name>:<dir>,... - set sort specs");
			System.out.println("showReq: shows current request");
			System.out.println("clear: clears current request");
			System.out.println("clearSelections: clears all selections");
			System.out.println("clearSelection <name>: clear selection specified");
			System.out.println("clearFacetSpecs: clears all facet specs");
			System.out.println("clearFacetSpec <name>: clears specified facetspec");
			System.out.println("browse - executes a search");
		}
		else if ("nodes".equalsIgnoreCase(cmd)){
			Set<Node> nodes = cluster.getNodes();
			for (Node node : nodes){
			    String url = node.getUrl();
				System.out.println("id: "+node.getId());
				System.out.println("addr: "+ url);
				System.out.println("partitions: "+ node.getPartitions().toString());
				System.out.println("availlable :"+node.isAvailable());
				System.out.println("=========================");
			}
		}
		else if ("query".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("query not defined.");
			}
			else{
				String qString = parsed[1];
				_reqBuilder.setQuery(qString);
			}
		}
		else if ("facetspec".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("facetspec not defined.");
			}
			else{
				try{
					String fspecString = parsed[1];
					String[] parts = fspecString.split(":");
					String name = parts[0];
					String fvalue=parts[1];
					String[] valParts = fvalue.split(",");
					if (valParts.length != 4){
						System.out.println("spec must of of the form <minhitcount>,<maxcount>,<isExpand>,<orderby>");
					}
					else{
						int minHitCount = 1;
						int maxCount = 5;
						boolean expand=false;
						FacetSortSpec sort = FacetSortSpec.OrderHitsDesc;
						try{
						   	minHitCount = Integer.parseInt(valParts[0]);
						}
						catch(Exception e){
							System.out.println("default min hitcount = 1 is applied.");
						}
						try{
							maxCount = Integer.parseInt(valParts[1]);
						}
						catch(Exception e){
							System.out.println("default maxCount = 5 is applied.");
						}
						try{
							expand =Boolean.parseBoolean(valParts[2]);
						}
						catch(Exception e){
							System.out.println("default expand=false is applied.");
						}
						
						if ("hits".equals(valParts[3])){
							sort = FacetSortSpec.OrderHitsDesc;
						}
						else{
							sort = FacetSortSpec.OrderValueAsc;
						}
						
						_reqBuilder.applyFacetSpec(name, minHitCount, maxCount, expand, sort);
					}
					
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
		}
		else if ("select".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("selection not defined.");
			}
			else{
				try{
					String selString = parsed[1];
					String[] parts = selString.split(":");
					String name = parts[0];
					String selList = parts[1];
					String[] sels = selList.split(",");
					for (String sel : sels){
						boolean isNot=false;
						String val = sel;
						if (sel.startsWith("!")){
							isNot=true;
							val = sel.substring(1);
						}
						if (val!=null && val.length() > 0){
							_reqBuilder.addSelection(name, val, isNot);
						}
					}
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
		}
		else if ("page".equalsIgnoreCase(cmd)){
			try{
				String pageString = parsed[1];
				String[] parts = pageString.split(":");
				_reqBuilder.setOffset(Integer.parseInt(parts[0]));
				_reqBuilder.setCount(Integer.parseInt(parts[1]));
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		else if ("clearFacetSpec".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("facet spec not defined.");
			}
			else{
				String name = parsed[1];
				_reqBuilder.clearFacetSpec(name);
			}
		}
		else if ("clearSelection".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("selection name not defined.");
			}
			else{
				String name = parsed[1];
				_reqBuilder.clearSelection(name);
			}
		}
		else if ("clearSelections".equalsIgnoreCase(cmd)){
			_reqBuilder.clearSelections();
		}
		else if ("clearFacetSpecs".equalsIgnoreCase(cmd)){
			_reqBuilder.clearFacetSpecs();
		}
		else if ("clear".equalsIgnoreCase(cmd)){
			_reqBuilder.clear();
		}
		else if ("showReq".equalsIgnoreCase(cmd)){
			SenseiRequest req = _reqBuilder.getRequest();
			if(req.toString() != null)
			  System.out.println(req.toString());
			else
              System.out.println("No query request yet..");
		}
		else if ("sort".equalsIgnoreCase(cmd)){
			if (parsed.length == 2){
				String sortString = parsed[1];
				String[] sorts = sortString.split(",");
				ArrayList<SortField> sortList = new ArrayList<SortField>();
				for (String sort : sorts){
					String[] sortParams = sort.split(":");
					boolean rev = false;
					if (sortParams.length>0){
					  String sortName = sortParams[0];
					  if (sortParams.length>1){
						try{
						  rev = Boolean.parseBoolean(sortParams[1]);
						}
						catch(Exception e){
							System.out.println(e.getMessage()+", default rev to false");
						}
					  }
					  sortList.add(new SortField(sortName,SortField.CUSTOM,rev));
					}
				}
				_reqBuilder.applySort(sortList.toArray(new SortField[sortList.size()]));
			}
			else{
				_reqBuilder.applySort(null);
			}
		}
		else if ("browse".equalsIgnoreCase(cmd)){
			try{
			  SenseiRequest req = _reqBuilder.getRequest();
			  String queryString = _reqBuilder.getQueryString();
			
			  SenseiResult res = _broker.browse(req);
			  if(res == null)
			    System.out.println("No results found !");
			  else {
			    String output = BrowseResultFormatter.formatResults(res);
			    System.out.println(output);
			  }
			}
			catch(Exception e){
			  e.printStackTrace();
			}
		}
		else{
			System.out.println("Unknown command: "+cmd+", do help for list of supported commands");
		}
	}

}
