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

package com.senseidb.facet.iterator.loadable;

import java.text.DecimalFormat;

public abstract class TermNumberList<T extends Number> extends TermValueList<T>
{

  private static final String DEFAULT_FORMATTING_STRING = "0000000000";
  protected ThreadLocal<DecimalFormat> _formatter = null;
  protected String _formatString = null;

  protected TermNumberList()
  {
    super();
    setFormatString(DEFAULT_FORMATTING_STRING);
  }

  protected TermNumberList(String formatString)
  {
    super();
    setFormatString(formatString);
  }

  protected TermNumberList(int capacity, String formatString)
  {
    super(capacity);
    setFormatString(formatString);
  }

  protected void setFormatString(String formatString)
  {
    _formatString = formatString;
    _formatter = new ThreadLocal<DecimalFormat>()
    {
      protected DecimalFormat initialValue()
      {
        if (_formatString != null)
        {
          return new DecimalFormat(_formatString);
        } else
        {
          return null;
        }

      }
    };
  }

  public String getFormatString()
  {
    return _formatString;
  }

  protected abstract Object parseString(String o);
  public abstract double getDoubleValue(int index);
  @Override
  public String format(Object o)
  {
    if (o == null)
      return null;
    if (o instanceof String)
    {
      o = parseString((String) o);
    }
    if (_formatter == null)
    {
      return String.valueOf(o);
    } else
    {
      DecimalFormat formatter = _formatter.get();
      if (formatter == null)
        return String.valueOf(o);
      return formatter.format(o);
    }
  }
}
