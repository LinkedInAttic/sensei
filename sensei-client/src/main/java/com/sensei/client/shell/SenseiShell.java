package com.sensei.client.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.lucene.search.SortField;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.remoting.RemoteConnectFailureException;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.sensei.search.req.SenseiQuery;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.svc.api.SenseiService;
import com.sensei.search.svc.impl.HttpRestSenseiServiceImpl;

public class SenseiShell {

	static SenseiService svc = null;
	
	static BrowseRequestBuilder _reqBuilder = new BrowseRequestBuilder();
	

	public static final String SENSEI_CLIENT_SVC_URL_PROP = "sensei.service.url";
	public static final String SENSEI_CLIENT_CONF_FILE = "sensei-client.conf";
	
	private static void shutdown(){
		svc.shutdown();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{

	    File confFile = null;
	    if (args.length < 1){
          System.out.println("no config specified. specify the config dir");
          return;
	    }

	    File confDir = new File(args[0]);
	    confFile = new File(confDir , SENSEI_CLIENT_CONF_FILE);
	    
	    Configuration conf = new PropertiesConfiguration(confFile);

	    // create the network client
	    String schema = conf.getString("sensei.service.http.schema","http");
	    String host = conf.getString("sensei.service.http.host");
	    int port = conf.getInt("sensei.service.http.port",8080);
	    String path = conf.getString("sensei.service.http.path","path");
	    svc = new HttpRestSenseiServiceImpl(schema,host,port,path); 
	    /*
	    HttpInvokerProxyFactoryBean springInvokerBean = new HttpInvokerProxyFactoryBean();
	    springInvokerBean.setServiceUrl(conf.getString(SenseiDefaults.SENSEI_CLIENT_SVC_URL_PROP));
	    springInvokerBean.setServiceInterface(SenseiService.class);
	    springInvokerBean.afterPropertiesSet();
	    svc = (SenseiService)(springInvokerBean.getObject());
       */
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				try {
					shutdown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		

		System.out.print("> ");
		BufferedReader cmdLineReader = new BufferedReader(new InputStreamReader(System.in));
		try{
			String line = cmdLineReader.readLine();
			while(true){
				try{
				  processCommand(line);
				}
				catch(Exception ne){
					ne.printStackTrace();
				}
				System.out.print("> ");
				line = cmdLineReader.readLine();
			}
			
		}
		catch(Exception ie){
			throw new Exception(ie.getMessage(),ie);
		}
	}
	
	static void processCommand(String line) throws InterruptedException, ExecutionException{
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
			System.out.println("\t- help - prints this message");
			System.out.println("\t- exit - quits");
			System.out.println("\t- info - prints system information");
			System.out.println("\t- query <query string> - sets query text, the current query text will be overwritten if it exists");
			System.out.println("\t- facetspec <name>:<minHitCount>:<maxCount>:<isExpanded>:<sort> - add facet spec, e.g. facetspec color:1,5,true,hits");
			System.out.println("\t- page <offset>:<count> - set paging parameters. count: the number of result to show; offset may start from 0.");
			System.out.println("\t- select <name>:<value1>,<value2>... - add selection, with ! in front of value indicates a not");
			System.out.println("\t- sort <name>:<dir>,... - set sort specs");
			System.out.println("\t- showReq - shows current request");
			System.out.println("\t- clear - clears current request");
			System.out.println("\t- clearSelections - clears all selections");
			System.out.println("\t- clearSelection <name> - clear selection specified");
			System.out.println("\t- clearFacetSpecs - clears all facet specs");
			System.out.println("\t- clearFacetSpec <name> - clears specified facetspec");
			System.out.println("\t- browse - executes a search");
		}
		else if ("info".equalsIgnoreCase(cmd)){
			try{
			  SenseiSystemInfo systemInfo = svc.getSystemInfo();
			  System.out.println("\nSystem Information:\n");
			  System.out.println(systemInfo+"\n");
			}
			catch(org.springframework.remoting.RemoteConnectFailureException conExcep){
			  System.out.println("Connection problem. Please make sure Sensei is running.");
			}
			catch(UnsupportedOperationException e){
			  System.out.println("Unsupportted command now.");
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		else if ("query".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("query not defined.");
			}
			else{
				String qString = line.substring(line.indexOf("query")+"query".length()).trim(); //parsed[1];
				_reqBuilder.setQuery(qString);
			}
		}
		else if ("facetspec".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("facetspec not defined.");
			}
			else{
				try{
					String fspecString = line.substring(line.indexOf("facetspec")+"facetspec".length()).trim(); //parsed[1];
					String[] parts = fspecString.split(":");
					String name = parts[0].trim();
					String fvalue=parts[1].trim();
					String[] valParts = fvalue.split(",\\s*");
					if (valParts.length != 4){
						System.out.println("spec must of of the form <minhitcount>,<maxcount>,<isExpand>,<orderby>");
					}
					else{
						int minHitCount = 1;
						int maxCount = 5;
						boolean expand=false;
						FacetSortSpec sort = FacetSortSpec.OrderHitsDesc;
						try{
						   	minHitCount = Integer.parseInt(valParts[0].trim());
						}
						catch(Exception e){
							System.out.println("default min hitcount = 1 is applied.");
						}
						try{
							maxCount = Integer.parseInt(valParts[1].trim());
						}
						catch(Exception e){
							System.out.println("default maxCount = 5 is applied.");
						}
						try{
							expand =Boolean.parseBoolean(valParts[2].trim());
						}
						catch(Exception e){
							System.out.println("default expand=false is applied.");
						}
						
						if ("hits".equals(valParts[3].trim())){
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
					String selString = line.substring(line.indexOf("select")+"select".length()).trim();
					String[] parts = selString.split(":");
					String name = parts[0];
					String selList = parts[1];
					String[] sels = selList.split(",\\s*");
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
      if (parsed.length<2){
        System.out.println("paging parameters are not specified.");
      }
      else{
  			try{
  				String pageString = line.substring(line.indexOf("page")+"page".length()).trim(); //parsed[1];
  				String[] parts = pageString.split(":");
  				_reqBuilder.setOffset(Integer.parseInt(parts[0].trim()));
  				_reqBuilder.setCount(Integer.parseInt(parts[1].trim()));
  			}
  			catch(Exception e){
  				e.printStackTrace();
  			}
      }
		}
		else if ("clearFacetSpec".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("facet spec not defined.");
			}
			else{
				String name = parsed[1].trim();
				_reqBuilder.clearFacetSpec(name);
			}
		}
		else if ("clearSelection".equalsIgnoreCase(cmd)){
			if (parsed.length<2){
				System.out.println("selection name not defined.");
			}
			else{
				String name = parsed[1].trim();
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
			if(req.toString() != null){

		    StringBuilder buf=new StringBuilder();
		    SenseiQuery _query = req.getQuery();
		    int _offset = req.getOffset();
		    int _count = req.getCount();
		    SortField[] _sortSpecs = req.getSort();
		    BrowseSelection[] _selections = req.getSelections();
		    Map _facetSpecMap = req.getFacetSpecs();
		    boolean _fetchStoredFields = req.isFetchStoredFields();
		    if(_query != null){
		      try
          {
            JSONObject query_json = new JSONObject(_query.toString());
            buf.append("\t- query: \"").append(query_json.optString("query").toString()).append("\"\n");
            buf.append("\t- page: [").append(_offset).append(',').append(_count).append("]\n");
            if(_sortSpecs != null)
              buf.append("\t- sort spec: ").append(arrayToString(_sortSpecs)).append('\n');
            if(_selections != null)
              buf.append("\t- selections: ").append(arrayToString(_selections)).append('\n');
            if(_facetSpecMap != null)
              buf.append("\t- facet spec: ").append(facetMapToString(_facetSpecMap)).append('\n');
            buf.append("\t- fetch stored fields: ").append(_fetchStoredFields);
            System.out.println(buf.toString());
          } catch (JSONException e)
          {
//            e.printStackTrace();
            System.out.println("Internal error when converting sensei query.");
          }

		    }
		    else
              System.out.println("No query request yet..");
			}
			else{
			  System.out.println("No query request yet..");
			}
		}
		else if ("sort".equalsIgnoreCase(cmd)){
      if (parsed.length >= 2)
      {
        String sortString = line.substring(line.indexOf("sort") + "sort".length()).trim(); // parsed[1];
        String[] sorts = sortString.split(",\\s*");
        ArrayList<SortField> sortList = new ArrayList<SortField>();
        for (String sort : sorts)
        {
          String[] sortParams = sort.split(":");
          boolean rev = false;
          if (sortParams.length > 0)
          {
            String sortName = sortParams[0];
            if (sortParams.length > 1)
            {
              try
              {
                rev = Boolean.parseBoolean(sortParams[1]);
              } catch (Exception e)
              {
                System.out.println(e.getMessage() + ", default rev to false");
              }
            }
            sortList.add(new SortField(sortName, SortField.CUSTOM, rev));
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
			  SenseiResult res = svc.doQuery(req);
			  if(res == null)
			    System.out.println("No results found !");
			  else {
			    String output = BrowseResultFormatter.formatResults(res);
			    System.out.println(output);
			  }
			}
			catch(RemoteConnectFailureException re){
			  System.out.println("Can not connect to the server.");
			}
			catch(Exception e){
			  e.printStackTrace();
			}
		}
		else{
			System.out.println("Unknown command: "+cmd+", do help for list of supported commands");
		}
	}
	
	/**
   * @param _facetSpecMap
   * @return
   */
  private static String facetMapToString(Map<String, FacetSpec> _facetSpecMap)
  {
    StringBuffer sb = new StringBuffer();
    Iterator<String> it = _facetSpecMap.keySet().iterator();
    while(it.hasNext()){
      String key = it.next();
      sb.append("\n\t  - "+key+": [ ");
      FacetSpec fs = _facetSpecMap.get(key);
      {
        sb.append("orderBy: ").append(fs.getOrderBy()).append(", ");
        sb.append("max count: ").append(fs.getMaxCount()).append(", ");
        sb.append("min hit count: ").append(fs.getMinHitCount()).append(", ");
        sb.append("expandSelection: ").append(fs.isExpandSelection());
      }
      sb.append(" ]");
    }
    return sb.toString();
  }

  /**
   * @param _selections
   * @return
   */
  private static String arrayToString(BrowseSelection[] _selections)
  {
    StringBuffer sb = new StringBuffer();
    for(BrowseSelection br : _selections){
      sb.append("\n\t  - ");
      sb.append("name: ").append(br.getFieldName()+", ");
      sb.append("values: "+ arrayToString(br.getValues())+", ");
      sb.append("nots: "+ arrayToString(br.getNotValues())+", ");
      sb.append("op: "+ br.getSelectionOperation().toString()+", ");
      sb.append("sel props: "+ br.getSelectionProperties().toString());
//      sb.append(br.toString()+" ");
    }
    return sb.toString();
  }

  private static String arrayToString(SortField[] _sortSpecs){
	  StringBuffer sb = new StringBuffer();
	  for(SortField sf : _sortSpecs){
	    sb.append(sf.getField()+":reverse="+sf.getReverse()+"  ");
	  }
	  return sb.toString().trim();
	}

  private static String arrayToString(String[] _stringArray){
    StringBuffer sb = new StringBuffer();
    for(String str : _stringArray){
      sb.append(str+" ");
    }
    return sb.toString().trim().length()>0 ? sb.toString().trim() : "N/A";
  }

}
