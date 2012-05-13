package com.senseidb.example.tweets.gateway;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.linkedin.zoie.api.DataConsumer.DataEvent;
import com.linkedin.zoie.impl.indexing.StreamDataProvider;

public class TwitterSampleStreamer extends StreamDataProvider<JSONObject> {

  private static Logger logger = Logger.getLogger(TwitterSampleStreamer.class);
  // following hashtag extraction logic taken from twitter-text-java: https://github.com/twitter/twitter-text-java
  private static String LATIN_ACCENTS_CHARS = "\\u00c0-\\u00d6\\u00d8-\\u00f6\\u00f8-\\u00ff\\u015f";

  private static final String HASHTAG_ALPHA_CHARS = "a-z" + LATIN_ACCENTS_CHARS +
      "\\u0400-\\u04ff\\u0500-\\u0527" +  // Cyrillic
      "\\u2de0–\\u2dff\\ua640–\\ua69f" +  // Cyrillic Extended A/B
      "\\u1100-\\u11ff\\u3130-\\u3185\\uA960-\\uA97F\\uAC00-\\uD7AF\\uD7B0-\\uD7FF" + // Hangul (Korean)
      "\\p{InHiragana}\\p{InKatakana}" +  // Japanese Hiragana and Katakana
      "\\p{InCJKUnifiedIdeographs}" +     // Japanese Kanji / Chinese Han
      "\\u3005\\u303b" +                  // Kanji/Han iteration marks
      "\\uff21-\\uff3a\\uff41-\\uff5a" +  // full width Alphabet
      "\\uff66-\\uff9f" +                 // half width Katakana
      "\\uffa1-\\uffdc";                  // half width Hangul (Korean)
  private static final String HASHTAG_ALPHA_NUMERIC_CHARS = "0-9\\uff10-\\uff19_" + HASHTAG_ALPHA_CHARS;
  private static final String HASHTAG_ALPHA = "[" + HASHTAG_ALPHA_CHARS +"]";
  private static final String HASHTAG_ALPHA_NUMERIC = "[" + HASHTAG_ALPHA_NUMERIC_CHARS +"]";

  public static final Pattern AUTO_LINK_HASHTAGS = Pattern.compile("(^|[^&/" + HASHTAG_ALPHA_NUMERIC_CHARS + "])(#|\uFF03)(" + HASHTAG_ALPHA_NUMERIC + "*" + HASHTAG_ALPHA + HASHTAG_ALPHA_NUMERIC + "*)", Pattern.CASE_INSENSITIVE);
  public static final Pattern HASHTAG_MATCH_END = Pattern.compile("^(?:[#＃]|://)");

  private static List<String> extractHashtags(String text) {
    List<String> extracted = new ArrayList<String>();
    Matcher matcher = AUTO_LINK_HASHTAGS.matcher(text);
    while (matcher.find()) {
      String after = text.substring(matcher.end());
      if (!HASHTAG_MATCH_END.matcher(after).find()) {
        extracted.add(matcher.group(3));
      }
    }
    return extracted;

  }

  private final BufferedReader _tweetReader;

  public TwitterSampleStreamer(Map<String, String> config,Comparator<String> versionComparator) throws Exception {
    super(versionComparator);
    String username = config.get("username");
    String password = config.get("password");

    URL url = new URL("https://stream.twitter.com/1/statuses/sample.json");
    URLConnection uc = url.openConnection();

    String userPassword = username+":"+password;

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

        String textString = jsonObj.optString("text","");

        long time = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(jsonObj.getString("created_at")).getTime();
        tweetJSON.put("time", time);
        JSONObject user = jsonObj.optJSONObject("user");
        String screenName = user.optString("screen_name","");
        tweetJSON.put("tweeter", screenName);

        List<String> hashtags = extractHashtags(textString);

        StringBuilder contentBuilder = new StringBuilder();
        contentBuilder.append(textString).append("\n");
        contentBuilder.append(screenName).append("\n");
        if(hashtags!=null && hashtags.size()>0){
          StringBuilder buf = new StringBuilder();
          boolean first = true;
          for (String tag : hashtags){
            if (!first){
              buf.append(",");
            }
            else{
              first = false;
            }
            buf.append(tag);
            contentBuilder.append(tag).append("\n");
          }
          tweetJSON.put("hashtags", buf.toString());
        }

        tweetJSON.put("contents", contentBuilder.toString());

        tweetJSON.put("tweet", jsonObj);
        tweetEvent = new DataEvent<JSONObject>(tweetJSON,String.valueOf(System.currentTimeMillis()));
        if (logger.isDebugEnabled()){
          logger.debug("event: "+tweetJSON.toString());
        }
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

  @Override
  public void stop() {
    try{
      _tweetReader.close();
    }
    catch(Exception e){
      logger.error(e.getMessage(),e);
    }
    finally{
      super.stop();
    }
  }
}
