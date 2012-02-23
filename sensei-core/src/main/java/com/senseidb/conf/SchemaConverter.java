package com.senseidb.conf;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class SchemaConverter
{
  private static Logger logger = Logger.getLogger(SchemaConverter.class);

  static public JSONObject convert(Document schemaDoc)
      throws ConfigurationException, JSONException
  {
    JSONObject jsonObj = new JSONObject();
    JSONObject tableObj = new JSONObject();
    jsonObj.put("table", tableObj);

    NodeList tables = schemaDoc.getElementsByTagName("table");
    if (tables != null && tables.getLength() > 0)
    {
      Element tableElem = (Element) tables.item(0);
      tableObj.put("uid", tableElem.getAttribute("uid"));
      String deleteField = tableElem.getAttribute("delete-field");
      if (deleteField != null)
        tableObj.put("delete-field", deleteField);

      String skipField = tableElem.getAttribute("skip-field");
      if (skipField != null)
        tableObj.put("skip-field", skipField);

      String srcDataStore = tableElem.getAttribute("src-data-store");
      if (srcDataStore != null)
        tableObj.put("src-data-store", srcDataStore);

      String srcDatafield = tableElem.getAttribute("src-data-field");
      if (srcDatafield == null || srcDatafield.length() == 0)
        srcDatafield = "src_data";
      tableObj.put("src-data-field", srcDatafield);

      String compress = tableElem.getAttribute("compress-src-data");
      if (compress != null && "false".equals(compress))
        tableObj.put("compress-src-data", false);
      else
        tableObj.put("compress-src-data", true);

      NodeList columns = tableElem.getElementsByTagName("column");
      JSONArray columnArray = new JSONArray();
      tableObj.put("columns", columnArray);

      for (int i = 0; i < columns.getLength(); ++i)
      {
        try
        {
          Element column = (Element) columns.item(i);
          JSONObject columnObj = new JSONObject();
          columnArray.put(columnObj);

          String n = column.getAttribute("name");
          String t = column.getAttribute("type");
          String frm = column.getAttribute("from");

          columnObj.put("name", n);
          columnObj.put("type", t);
          columnObj.put("from", frm);

          columnObj.put("multi", Boolean.parseBoolean(column.getAttribute("multi")));

          String delimString = column.getAttribute("delimiter");
          if (delimString != null && delimString.trim().length() > 0)
          {
            columnObj.put("delimiter", delimString);
          }

          String f = "";
          try
          {
            f = column.getAttribute("format");
          }
          catch (Exception ex)
          {
            logger.error(ex.getMessage(), ex);
          }
          if (!f.isEmpty())
            columnObj.put("format", f);

          String idxString = column.getAttribute("index");
          if (idxString != null)
          {
            columnObj.put("index", idxString);
          }
          String storeString = column.getAttribute("store");
          if (storeString != null)
          {
            columnObj.put("store", storeString);
          }
          String tvString = column.getAttribute("termvector");
          if (tvString != null)
          {
            columnObj.put("termvector", tvString);
          }

        }
        catch (Exception e)
        {
          throw new ConfigurationException("Error parsing schema: " + columns.item(i), e);
        }
      }
    }


    NodeList facets = schemaDoc.getElementsByTagName("facet");
    JSONArray facetArray = new JSONArray();
    jsonObj.put("facets", facetArray);

    for (int i = 0; i < facets.getLength(); ++i)
    {
      try
      {
        Element facet = (Element) facets.item(i);
        JSONObject facetObj = new JSONObject();
        facetArray.put(facetObj);

        facetObj.put("name", facet.getAttribute("name"));
        facetObj.put("type", facet.getAttribute("type"));
        String depends = facet.getAttribute("depends");
        if (depends!=null){
          facetObj.put("depends", depends);
        }
        String column = facet.getAttribute("column");
        if (column!=null && column.length() > 0){
          facetObj.put("column", column);
        }
        String dynamic = facet.getAttribute("dynamic");
        if (dynamic!=null){
          facetObj.put("dynamic",dynamic);
        }

        NodeList paramList = facet.getElementsByTagName("param");
        if (paramList!=null){
          JSONArray params = new JSONArray();
          facetObj.put("params", params);
          for (int j = 0; j < paramList.getLength(); ++j) {
            Element param = (Element) paramList.item(j);
            String paramName = param.getAttribute("name");
            String paramValue = param.getAttribute("value");
            JSONObject paramObj = new JSONObject();
            paramObj.put("name", paramName);
            paramObj.put("value", paramValue);
            params.put(paramObj);
          }
        }
      }
      catch(Exception e){
        throw new ConfigurationException("Error parsing schema: " + facets.item(i), e);
      }
    }

    return jsonObj;
  }

  public static void main(String[] args) throws Exception
  {
    File xmlSchema = new File("../example/cars/conf/schema.xml");
    if (!xmlSchema.exists()){
      throw new ConfigurationException("schema not file");
    }
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setIgnoringComments(true);
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document schemaXml = db.parse(xmlSchema);
    schemaXml.getDocumentElement().normalize();
    JSONObject json = SchemaConverter.convert(schemaXml);
    System.out.println(json.toString(4));
  }
}
