package com.senseidb.test;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import org.apache.http.NameValuePair;


/**
 * Created by IntelliJ IDEA. User: bguarrac Date: 4/8/11 Time: 3:47 PM To change this template use File | Settings |
 * File Templates.
 */
public class MockServletRequest implements ServletRequest
{
  Map<String, List<String>> _map;

  public MockServletRequest(Map<String, List<String>> map) {
    _map = map;
  }

  public static MockServletRequest create(List<NameValuePair> list) {
    Map<String, List<String>> map = new HashMap<String, List<String>>();

    for (NameValuePair pair : list) {
      String name = pair.getName();
      if (!map.containsKey(name)) {
        map.put(name, new ArrayList<String>());
      }

      for (String value: pair.getValue().split(","))
      {
        map.get(name).add(value);
      }
    }

    return new MockServletRequest(map);
  }

  @Override
  public Object getAttribute(String s)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Enumeration getAttributeNames()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCharacterEncoding()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCharacterEncoding(String s)
      throws UnsupportedEncodingException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getContentLength()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getContentType()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ServletInputStream getInputStream()
      throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getParameter(String s)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Enumeration getParameterNames()
  {
    return new EnumerationWrapper(_map.keySet().iterator());
  }

  public class EnumerationWrapper implements Enumeration {

    Iterator _iterator;

    public EnumerationWrapper(Iterator iter) {
      _iterator = iter;
    }

    @Override
    public boolean hasMoreElements()
    {
      return _iterator.hasNext();
    }

    @Override
    public Object nextElement()
    {
      return _iterator.next();
    }
  };

  @Override
  public String[] getParameterValues(String s)
  {
    List<String> value = _map.get(s);
    if (value == null)
      return new String[0];
    return value.toArray(new String[value.size()]);
  }

  @Override
  public Map getParameterMap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getProtocol()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getScheme()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getServerName()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getServerPort()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferedReader getReader()
      throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRemoteAddr()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRemoteHost()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAttribute(String s, Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeAttribute(String s)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Locale getLocale()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Enumeration getLocales()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSecure()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String s)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRealPath(String s)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRemotePort()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLocalName()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLocalAddr()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLocalPort()
  {
    throw new UnsupportedOperationException();
  }
}
