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
package com.senseidb.bql.parsers;

import java.util.HashMap;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.json.JSONObject;

import com.senseidb.bql.parsers.BQLLexer;
import com.senseidb.bql.parsers.BQLParser;

public class BQLCompiler extends AbstractCompiler
{
  // A map containing facet type and data type info for a facet
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();
  private BQLParser _parser = null;

  public BQLCompiler(Map<String, String[]> facetInfoMap)
  {
    _facetInfoMap = facetInfoMap;
  }

  public JSONObject compile(String bqlStmt) throws RecognitionException
  {
    // Lexer splits input into tokens
    ANTLRStringStream input = new ANTLRStringStream(bqlStmt);
    TokenStream tokens = new CommonTokenStream(new BQLLexer(input));
      
    // Parser generates abstract syntax tree
    _parser = new BQLParser(tokens, _facetInfoMap);
    BQLParser.statement_return ret = _parser.statement();
      
    // Acquire parse result
    CommonTree ast = (CommonTree) ret.tree;

    JSONObject json = (JSONObject) ret.json;
    // XXX To be removed
    // printTree(ast);
    // System.out.println(">>> json = " + json.toString());
    return json;
  }

  public String getErrorMessage(RecognitionException error)
  {
    if (_parser != null)
    {
      return _parser.getErrorMessage(error, _parser.getTokenNames());
    }
    else
    {
      return null;
    }
  }
}
