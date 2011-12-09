package com.senseidb.example.tweets.gateway;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public class TwitterSampleStreamer extends StreamDataProvider<JSONObject> {
  
  private static Logger logger = Logger.getLogger(TwitterSampleStreamer.class);

  private final BufferedReader _tweetReader;
  public TwitterSampleStreamer(Configuration conf,Comparator<String> versionComparator) throws Exception {
    super(versionComparator);
    String username = conf.getString("username");
    String password = conf.getString("password");
    
    URL url = new URL("https://stream.twitter.com/1/statuses/sample.json");
    URLConnection uc = url.openConnection();
    
    String userPassword = username+":"+password;
    logger.info("user/pass: "+userPassword);
    String encoding = new sun.misc.BASE64Encoder().encode (userPassword.getBytes());
    uc.setRequestProperty ("Authorization", "Basic " + encoding);
    
    
    InputStream in = uc.getInputStream();
    _tweetReader =new BufferedReader(new InputStreamReader(in,"UTF-8"));
  }

  @Override
  public DataEvent<JSONObject> next() {
    DataEvent<JSONObject> tweetEvent = null;
    try{
      String tweet = _tweetReader.readLine();
      logger.info("tweet: "+tweet);
      JSONObject jsonObj = new JSONObject(tweet);
      String id = jsonObj.optString("id_str", null);
      if (id!=null){
        JSONObject tweetJSON = new JSONObject();
        tweetJSON.put("id", Long.parseLong(id));
        tweetJSON.put("contents", jsonObj.optString("text",""));
        long time = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(jsonObj.getString("created_at")).getTime();
        tweetJSON.put("time", time);
        tweetEvent = new DataEvent<JSONObject>(tweetJSON,String.valueOf(time));
        logger.info("event: "+tweetJSON.toString());
      }
    }
    catch(Exception e){
      logger.error(e.getMessage(),e);
      return null;
    }
    return tweetEvent;
  }

  @Override
  public void setStartingOffset(String version) {
    
  }

  @Override
  public void reset() {
    
  }

}
