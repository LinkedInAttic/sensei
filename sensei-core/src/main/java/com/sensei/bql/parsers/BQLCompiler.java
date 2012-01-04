package com.sensei.bql.parsers;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;

import org.json.JSONObject;
import org.json.JSONException;

import com.sensei.search.req.SenseiSystemInfo;

public class BQLCompiler extends AbstractCompiler
{
  // A map containing facet type and data type info for a facet
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();

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
    BQLParser parser = new BQLParser(tokens, _facetInfoMap);
    BQLParser.statement_return ret = parser.statement();
      
    // Acquire parse result
    CommonTree ast = (CommonTree) ret.tree;
    printTree(ast);
    JSONObject json = (JSONObject) ret.json;
    System.out.println(">>> json = " + json.toString());
    return json;
  }
}
