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
package com.senseidb.search.query.filters;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TermRangeFilter;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.query.MatchAllDocIdSetIterator;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.MetaType;
import com.senseidb.indexing.activity.facet.ActivityRangeFacetHandler;

public class RangeFilterConstructor extends FilterConstructor
{
  public static final String FILTER_TYPE = "range";

  @Override
  protected SenseiFilter doConstructFilter(Object obj) throws Exception
  {
    final JSONObject json = (JSONObject)obj;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    final String field = iter.next();

    JSONObject jsonObj = json.getJSONObject(field);

    final String  from, to;
    final boolean include_lower, include_upper;
    final boolean noOptimize = jsonObj.optBoolean(NOOPTIMIZE_PARAM, false);
    final String type;
    final String dateFormat;
    
    String gt  = jsonObj.optString(GT_PARAM, null);
    String gte = jsonObj.optString(GTE_PARAM, null);
    String lt  = jsonObj.optString(LT_PARAM, null);
    String lte = jsonObj.optString(LTE_PARAM, null);
    
    type = jsonObj.optString(RANGE_FIELD_TYPE, null);
    dateFormat = jsonObj.optString(RANGE_DATE_FORMAT, null);
    
    if (gt != null && gt.length() != 0)
    {
      from          = gt;
      include_lower = false;
    }
    else if (gte != null && gte.length() != 0)
    {
      from          = gte;
      include_lower = true;
    }
    else
    {
      from          = jsonObj.optString(FROM_PARAM, null);
      include_lower = jsonObj.optBoolean(INCLUDE_LOWER_PARAM, true);
    }

    if (lt != null && lt.length() != 0)
    {
      to = lt;
      include_upper = false;
    }
    else if (lte != null && lte.length() != 0)
    {
      to = lte;
      include_upper = true;
    }
    else
    {
      to            = jsonObj.optString(TO_PARAM, null);
      include_upper = jsonObj.optBoolean(INCLUDE_UPPER_PARAM, true);
    }

    return new SenseiFilter()
    {
      @Override
      public SenseiDocIdSet getSenseiDocIdSet(final IndexReader reader) throws IOException {
        DocIdSetCardinality defaultDocIdSetCardinalityEstimate = DocIdSetCardinality.random();

        String fromPadded = from, toPadded = to;
        if (!noOptimize)
        {
          if (reader instanceof BoboIndexReader)
          {
            final BoboIndexReader boboReader = (BoboIndexReader)reader;
            FacetHandler facetHandler = boboReader.getFacetHandler(field);
            if (facetHandler != null)
            {
              StringBuilder sb = new StringBuilder();
              if (include_lower && from != null && from.length() != 0)
                sb.append("[");
              else
                sb.append("(");

              if (from == null || from.length() == 0)
                sb.append("*");
              else
                sb.append(from);
              sb.append(" TO ");
              if (to == null || to.length() == 0)
                sb.append("*");
              else
                sb.append(to);

              if (include_upper && to != null && to.length() != 0)
                sb.append("]");
              else
                sb.append(")");
              RandomAccessFilter filter = null;;
              if (facetHandler instanceof ActivityRangeFacetHandler) {
            	  filter = ((ActivityRangeFacetHandler) facetHandler).buildRandomAccessFilter(sb.toString(), null);
              } else {
            	  filter = new FacetRangeFilter(facetHandler, sb.toString());
              }
              DocIdSet docIdSet = filter.getDocIdSet(reader);
              return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.exact(filter.getFacetSelectivity(boboReader)), "RANGE " + field + sb.toString());
            }
          }
        }
        
        if(type == null)
          throw new IllegalArgumentException("need to specify the type of field in filter json: " + json);
        
        if ("int".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fromPadded = formatter.format(Integer.parseInt(from));
          toPadded = formatter.format(Integer.parseInt(to));
        } 
        else if ("short".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fromPadded = formatter.format(Short.parseShort(from));
          toPadded = formatter.format(Short.parseShort(to));
        }
        else if ("long".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fromPadded = formatter.format(Long.parseLong(from));
          toPadded = formatter.format(Long.parseLong(to));
        }
        else if ("date".equals(type)) {
          if(dateFormat == null)
            throw new IllegalArgumentException("Date format cannot be empty in filter json when type is date: " + json);
          else{
            SimpleDateFormat  formatter = new SimpleDateFormat(dateFormat);
            fromPadded = formatter.format(new Date(Long.parseLong(from)));
            toPadded = formatter.format(new Date(Long.parseLong(to)));
          }
        }
        else if ("float".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fromPadded = formatter.format(Float.parseFloat(from));
          toPadded = formatter.format(Float.parseFloat(to));
        }
        else if ("double".equals(type)) {
          MetaType metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
          String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
          DecimalFormat formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
          fromPadded = formatter.format(Double.parseDouble(from));
          toPadded = formatter.format(Double.parseDouble(to));
        }
        
        if (fromPadded == null || fromPadded.length() == 0)
          if (toPadded == null || toPadded.length() == 0)
            return new SenseiDocIdSet(new DocIdSet()
            {
              @Override
              public boolean isCacheable()
              {
                return false;
              }

              @Override
              public DocIdSetIterator iterator() throws IOException
              {
                return new MatchAllDocIdSetIterator(reader);
              }

            }, DocIdSetCardinality.one(), "ALL");
          else
            return new SenseiDocIdSet(new TermRangeFilter(field, fromPadded, toPadded, false,
                include_upper).getDocIdSet(reader), defaultDocIdSetCardinalityEstimate, "RANGE " + field + " TO " + toPadded);
        else if (toPadded == null|| toPadded.length() == 0)
          return new SenseiDocIdSet(new TermRangeFilter(field, fromPadded, toPadded, include_lower,
              false).getDocIdSet(reader), defaultDocIdSetCardinalityEstimate, "RANGE " + field + " FROM " + fromPadded);

        return new SenseiDocIdSet(new TermRangeFilter(field, fromPadded, toPadded, include_lower,
          include_upper).getDocIdSet(reader), defaultDocIdSetCardinalityEstimate, "RANGE " + field + " FROM " + fromPadded + " TO " + toPadded);
      }
    };
  }
}
