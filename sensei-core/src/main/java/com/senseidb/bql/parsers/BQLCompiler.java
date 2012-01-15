package com.senseidb.bql.parsers;

import java.util.HashMap;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.json.JSONObject;

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
