package com.sensei.test;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


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
      if (!map.containsKey(pair.getName())) {
        map.put(pair.getName(), new ArrayList<String>());
      }
      map.get(pair.getName()).add(pair.getValue());
    }

    return new MockServletRequest(map);
  }

  @Override
  public Object getAttribute(String s)
  {
    throw new NotImplementedException();
  }

  @Override
  public Enumeration getAttributeNames()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getCharacterEncoding()
  {
    throw new NotImplementedException();
  }

  @Override
  public void setCharacterEncoding(String s)
      throws UnsupportedEncodingException
  {
    throw new NotImplementedException();
  }

  @Override
  public int getContentLength()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getContentType()
  {
    throw new NotImplementedException();
  }

  @Override
  public ServletInputStream getInputStream()
      throws IOException
  {
    throw new NotImplementedException();
  }

  @Override
  public String getParameter(String s)
  {
    throw new NotImplementedException();
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
    return value.toArray(new String[value.size()]);
  }

  @Override
  public Map getParameterMap()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getProtocol()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getScheme()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getServerName()
  {
    throw new NotImplementedException();
  }

  @Override
  public int getServerPort()
  {
    throw new NotImplementedException();
  }

  @Override
  public BufferedReader getReader()
      throws IOException
  {
    throw new NotImplementedException();
  }

  @Override
  public String getRemoteAddr()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getRemoteHost()
  {
    throw new NotImplementedException();
  }

  @Override
  public void setAttribute(String s, Object o)
  {
    throw new NotImplementedException();
  }

  @Override
  public void removeAttribute(String s)
  {
    throw new NotImplementedException();
  }

  @Override
  public Locale getLocale()
  {
    throw new NotImplementedException();
  }

  @Override
  public Enumeration getLocales()
  {
    throw new NotImplementedException();
  }

  @Override
  public boolean isSecure()
  {
    throw new NotImplementedException();
  }

  @Override
  public RequestDispatcher getRequestDispatcher(String s)
  {
    throw new NotImplementedException();
  }

  @Override
  public String getRealPath(String s)
  {
    throw new NotImplementedException();
  }

  @Override
  public int getRemotePort()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getLocalName()
  {
    throw new NotImplementedException();
  }

  @Override
  public String getLocalAddr()
  {
    throw new NotImplementedException();
  }

  @Override
  public int getLocalPort()
  {
    throw new NotImplementedException();
  }
}
