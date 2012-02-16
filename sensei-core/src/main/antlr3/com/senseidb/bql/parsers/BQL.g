grammar BQL;

options
{
  // ANTLR will generate java lexer and parser
  language = Java;
  // Generated parser should create abstract syntax tree
  output = AST;
}

// Imaginary tokens
tokens 
{
    COLUMN_LIST;
    OR_PRED;
    AND_PRED;
    EQUAL_PRED;
    RANGE_PRED;
}

// As the generated lexer will reside in com.senseidb.bql.parsers package,
// we have to add package declaration on top of it
@lexer::header {
package com.senseidb.bql.parsers;
}

@lexer::members {

  // @Override
  // public void reportError(RecognitionException e) {
  //   throw new IllegalArgumentException(e);
  // }

}

// As the generated parser will reside in com.senseidb.bql.parsers
// package, we have to add package declaration on top of it
@parser::header {
package com.senseidb.bql.parsers;

import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
}

@parser::members {

    private static final int DEFAULT_REQUEST_OFFSET = 0;
    private static final int DEFAULT_REQUEST_COUNT = 10;
    private static final int DEFAULT_REQUEST_MAX_PER_GROUP = 10;
    private static final int DEFAULT_FACET_MINHIT = 1;
    private static final int DEFAULT_FACET_MAXHIT = 10;

    Map<String, String[]> _facetInfoMap;
    private long _now;
    private HashSet<String> _variables;
    private SimpleDateFormat[] _format1 = new SimpleDateFormat[2];
    private SimpleDateFormat[] _format2 = new SimpleDateFormat[2];

    public BQLParser(TokenStream input, Map<String, String[]> facetInfoMap)
    {
        this(input);
        _facetInfoMap = facetInfoMap;
        _facetInfoMap.put("_uid", new String[]{"simple", "long"});
    }

    // The following two overridden methods are used to force ANTLR to
    // stop parsing upon the very first error.

    // @Override
    // protected void mismatch(IntStream input, int ttype, BitSet follow)
    //     throws RecognitionException
    // {
    //     throw new MismatchedTokenException(ttype, input);
    // }

    @Override
    protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
        throws RecognitionException
    {
        throw new MismatchedTokenException(ttype, input);
    }

    @Override
    public Object recoverFromMismatchedSet(IntStream input,
                                           RecognitionException e,
                                           BitSet follow)
        throws RecognitionException
    {
        throw e;
    }

    @Override
    public String getErrorMessage(RecognitionException err, String[] tokenNames) 
    {
        // List stack = getRuleInvocationStack(err, this.getClass().getName());
        String msg = null; 
        if (err instanceof NoViableAltException) {
            NoViableAltException nvae = (NoViableAltException) err;
            // msg = "No viable alt; token=" + err.token.getText() +
            //     " (decision=" + nvae.decisionNumber +
            //     " state "+nvae.stateNumber+")" +
            //     " decision=<<" + nvae.grammarDecisionDescription + ">>";
            msg = "[line:" + err.line + ", col:" + err.charPositionInLine + "] " +
                "No viable alternative (token=" + err.token.getText() + ")";
        }
        else if (err instanceof MismatchedTokenException) {
            MismatchedTokenException mte = (MismatchedTokenException) err;
            String tokenName = (mte.expecting == Token.EOF) ? "EOF" : tokenNames[mte.expecting];
            msg = "[line:" + mte.line + ", col:" + mte.charPositionInLine + "] " +
                "Expecting " + tokenName +
                " (token=" + err.token.getText() + ")";
        }
        else if (err instanceof FailedPredicateException) {
            FailedPredicateException fpe = (FailedPredicateException) err;
            msg = "[line:" + fpe.line + ", col:" + fpe.charPositionInLine + "] " +
                fpe.predicateText +
                " (token=" + fpe.token.getText() + ")";
        }
        else {
            msg = super.getErrorMessage(err, tokenNames); 
        }
        return msg;
    } 

    @Override
    public String getTokenErrorDisplay(Token t)
    {
        return t.toString();
    }

    private String predType(JSONObject pred)
    {
        return (String) pred.keys().next();
    }

    private String predField(JSONObject pred) throws JSONException
    {
        String type = (String) pred.keys().next();
        JSONObject fieldSpec = pred.getJSONObject(type);
        return (String) fieldSpec.keys().next();
    }

    private boolean verifyFacetType(final String field, final String expectedType)
    {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            return (expectedType.equals(facetInfo[0]) || "custom".equals(facetInfo[0]));
        }
        else {
            return true;
        }
    }

    private boolean verifyValueType(Object value, final String columnType)
    {
        if (value instanceof String &&
            !((String) value).isEmpty() &&
            ((String) value).matches("\\$[^$].*"))
        {
            // This "value" is a variable, return true always
            return true;
        }

        if (columnType.equals("long") || columnType.equals("int") || columnType.equals("short")) {
            return !(value instanceof Float || value instanceof String || value instanceof Boolean);
        }
        else if (columnType.equals("float") || columnType.equals("double")) {
            return !(value instanceof String || value instanceof Boolean);
        }
        else if (columnType.equals("string")) {
            return (value instanceof String);
        }
        else if (columnType.equals("boolean")) {
            return (value instanceof Boolean);
        }
        else if (columnType.isEmpty()) {
            // For a custom facet, the data type is unknown (empty
            // string).  We accept all value types here.
            return true;
        }
        else {
            return false;
        }
    }

    private boolean verifyFieldDataType(final String field, Object value)
        throws FailedPredicateException
    {
        String[] facetInfo = _facetInfoMap.get(field);

        if (value instanceof String &&
            !((String) value).isEmpty() &&
            ((String) value).matches("\\$[^$].*"))
        {
            // This "value" is a variable, return true always
            return true;
        }
        else if (value instanceof JSONArray)
        {
            try {
                if (facetInfo != null) {
                    String columnType = facetInfo[1];
                    for (int i = 0; i < ((JSONArray) value).length(); ++i) {
                        if (!verifyValueType(((JSONArray) value).get(i), columnType)) {
                            return false;
                        }
                    }
                }
                return true;
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "verifyFieldDataType", "JSONException: " + err.getMessage());
            }
        }
        else
        {
            if (facetInfo != null) {
                return verifyValueType(value, facetInfo[1]);
            }
            else {
                // Field is not a facet
                return true;
            }
        }
    }

    private boolean verifyFieldDataType(final String field, Object[] values)
    {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            String columnType = facetInfo[1];
            for (Object value: values) {
                if (!verifyValueType(value, columnType)) {
                    return false;
                }
            }
        }
        return true;
    }

    private void extractSelectionInfo(JSONObject where,
                                      JSONArray selections,
                                      JSONObject filter,
                                      JSONObject query) throws JSONException {

        JSONObject queryPred = where.optJSONObject("query");
        JSONArray andPreds = null;

        if (queryPred != null) {
            query.put("query", queryPred);
        }
        else if ((andPreds = where.optJSONArray("and")) != null) {
            JSONArray filter_list = new JSONArray();
            for (int i = 0; i < andPreds.length(); ++i) {
                JSONObject pred = andPreds.getJSONObject(i);
                queryPred = pred.optJSONObject("query");
                if (queryPred != null) {
                    if (!query.has("query")) {
                        query.put("query", queryPred);
                    }
                    else {
                        filter_list.put(pred);
                    }
                }
                else if (pred.has("and") || pred.has("or") || pred.has("isNull")) {
                    filter_list.put(pred);
                }
                else if (_facetInfoMap.get(predField(pred)) != null) {
                    selections.put(pred);
                }
                else {
                    filter_list.put(pred);
                }
            }
            if (filter_list.length() > 1) {
                filter.put("filter", new JSONObject().put("and", filter_list));
            }
            else if (filter_list.length() == 1) {
                filter.put("filter", filter_list.get(0));
            }
        }
        else if (where.has("or") || where.has("isNull")) {
            filter.put("filter", where);
        }
        else if (_facetInfoMap.get(predField(where)) != null) {
            selections.put(where);
        }
        else {
            filter.put("filter", where);
        }
    }

    private int compareValues(Object v1, Object v2)
    {
        if (v1 instanceof String) {
            return ((String) v1).compareTo((String) v2);
        }
        else if (v1 instanceof Integer) {
            return ((Integer) v1).compareTo((Integer) v2);
        }
        else if (v1 instanceof Long) {
            return ((Long) v1).compareTo((Long) v2);
        }
        else if (v1 instanceof Float) {
            return ((Float) v1).compareTo((Float) v2);
        }
        return 0;
    }

    private Object[] getMax(Object value1, boolean include1, Object value2, boolean include2)
    {
        Object value;
        Boolean include;
        if (value1 == null) {
            value = value2;
            include = include2;
        }
        else if (value2 == null) {
            value = value1;
            include = include1;
        }
        else {
            int comp = compareValues(value1, value2);
            if (comp > 0) {
                value = value1;
                include = include1;
            }
            else if (comp == 0) {
                value = value1;
                include = (include1 && include2);
            }
            else {
                value = value2;
                include = include2;
            }
        }
        return new Object[]{value, include};
    }

    private Object[] getMin(Object value1, boolean include1, Object value2, boolean include2)
    {
        Object value;
        Boolean include;
        if (value1 == null) {
            value = value2;
            include = include2;
        }
        else if (value2 == null) {
            value = value1;
            include = include1;
        }
        else {
            int comp = compareValues(value1, value2);
            if (comp > 0) {
                value = value2;
                include = include2;
            }
            else if (comp == 0) {
                value = value1;
                include = (include1 && include2);
            }
            else {
                value = value1;
                include = include1;
            }
        }
        return new Object[]{value, include};
    }

    private void accumulateRangePred(TokenStream input, JSONObject fieldMap, JSONObject pred) 
        throws JSONException, RecognitionException
    {
        String field = predField(pred);
        if (!fieldMap.has(field)) {
            fieldMap.put(field, pred);
            return;
        }
        JSONObject oldRange = (JSONObject) fieldMap.get(field);
        JSONObject oldSpec = (JSONObject) ((JSONObject) oldRange.get("range")).get(field);
        Object oldFrom = oldSpec.opt("from");
        Object oldTo = oldSpec.opt("to");
        Boolean oldIncludeLower = oldSpec.optBoolean("include_lower", false);
        Boolean oldIncludeUpper = oldSpec.optBoolean("include_upper", false);

        JSONObject curSpec = (JSONObject) ((JSONObject) pred.get("range")).get(field);
        Object curFrom = curSpec.opt("from");
        Object curTo = curSpec.opt("to");
        Boolean curIncludeLower = curSpec.optBoolean("include_lower", false);
        Boolean curIncludeUpper = curSpec.optBoolean("include_upper", false);

        Object[] result = getMax(oldFrom, oldIncludeLower, curFrom, curIncludeLower);
        Object newFrom = result[0];
        Boolean newIncludeLower = (Boolean) result[1];
        result = getMin(oldTo, oldIncludeUpper, curTo, curIncludeUpper);
        Object newTo = result[0];
        Boolean newIncludeUpper = (Boolean) result[1];

        if (newFrom != null && newTo != null) {
            if (compareValues(newFrom, newTo) > 0 ||
                (compareValues(newFrom, newTo) == 0) && (!newIncludeLower || !newIncludeUpper)) {
                // This error is in general detected late, so the token
                // can be a little off, but hopefully the col index info
                // is good enough.
                throw new FailedPredicateException(input, "", "Inconsistent ranges detected for column: " + field);
            }
        }
        
        JSONObject newSpec = new JSONObject();
        if (newFrom != null) {
            newSpec.put("from", newFrom);
            newSpec.put("include_lower", newIncludeLower);
        }
        if (newTo != null) {
            newSpec.put("to", newTo);
            newSpec.put("include_upper", newIncludeUpper);
        }

        fieldMap.put(field, new JSONObject().put("range",
                                                 new JSONObject().put(field, newSpec)));
    }
}

@rulecatch {
    catch (RecognitionException err) {
        throw err;
    }
}

fragment DIGIT : '0'..'9' ;
fragment ALPHA : 'a'..'z' | 'A'..'Z' ;

INTEGER : DIGIT+ ;
REAL : DIGIT+ '.' DIGIT+ ;
LPAR : '(' ;
RPAR : ')' ;
COMMA : ',' ;
COLON : ':' ;
SEMI : ';' ;
EQUAL : '=' ;
GT : '>' ;
GTE : '>=' ;
LT : '<' ;
LTE : '<=';
NOT_EQUAL : '<>' ;

STRING_LITERAL
    :   ('"'
            { StringBuilder builder = new StringBuilder(); }
            ('"' '"'               { builder.appendCodePoint('"'); }
            | ch=~('"'|'\r'|'\n')  { builder.appendCodePoint(ch); }
            )*
         '"'
            { setText(builder.toString()); }
        )
    |
        ('\''
            { StringBuilder builder = new StringBuilder(); }
            ('\'' '\''             { builder.appendCodePoint('\''); }
            | ch=~('\''|'\r'|'\n') { builder.appendCodePoint(ch); }
            )*
         '\''
            { setText(builder.toString()); }
        )
    ;

DATE
    :   DIGIT DIGIT DIGIT DIGIT ('-'|'/') DIGIT DIGIT ('-'|'/') DIGIT DIGIT 
    ;

TIME
    :
        DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT
    ;

//
// BQL Keywords
//

ALL : ('A'|'a')('L'|'l')('L'|'l') ;
AFTER : ('A'|'a')('F'|'f')('T'|'t')('E'|'e')('R'|'r') ;
AGAINST : ('A'|'a')('G'|'g')('A'|'a')('I'|'i')('N'|'n')('S'|'s')('T'|'t') ;
AGO : ('A'|'a')('G'|'g')('O'|'o') ;
AND : ('A'|'a')('N'|'n')('D'|'d') ;
ASC : ('A'|'a')('S'|'s')('C'|'c') ;
BEFORE : ('B'|'b')('E'|'e')('F'|'f')('O'|'o')('R'|'r')('E'|'e') ;
BETWEEN : ('B'|'b')('E'|'e')('T'|'t')('W'|'w')('E'|'e')('E'|'e')('N'|'n') ;
BOOLEAN : ('B'|'b')('O'|'o')('O'|'o')('L'|'l')('E'|'e')('A'|'a')('N'|'n') ;
BROWSE : ('B'|'b')('R'|'r')('O'|'o')('W'|'w')('S'|'s')('E'|'e') ;
BY : ('B'|'b')('Y'|'y') ;
BYTEARRAY : ('B'|'b')('Y'|'y')('T'|'t')('E'|'e')('A'|'a')('R'|'r')('R'|'r')('A'|'a')('Y'|'y') ;
CONTAINS : ('C'|'c')('O'|'o')('N'|'n')('T'|'t')('A'|'a')('I'|'i')('N'|'n')('S'|'s') ;
DESC : ('D'|'d')('E'|'e')('S'|'s')('C'|'c') ;
DESCRIBE : ('D'|'d')('E'|'e')('S'|'s')('C'|'c')('R'|'r')('I'|'i')('B'|'b')('E'|'e') ;
DOUBLE : ('D'|'d')('O'|'o')('U'|'u')('B'|'b')('L'|'l')('E'|'e') ;
EXCEPT : ('E'|'e')('X'|'x')('C'|'c')('E'|'e')('P'|'p')('T'|'t') ;
FACET : ('F'|'f')('A'|'a')('C'|'c')('E'|'e')('T'|'t') ;
FALSE : ('F'|'f')('A'|'a')('L'|'l')('S'|'s')('E'|'e') ;
FETCHING : ('F'|'f')('E'|'e')('T'|'t')('C'|'c')('H'|'h')('I'|'i')('N'|'n')('G'|'g') ;
FROM : ('F'|'f')('R'|'r')('O'|'o')('M'|'m') ;
GROUP : ('G'|'g')('R'|'r')('O'|'o')('U'|'u')('P'|'p') ;
GIVEN : ('G'|'g')('I'|'i')('V'|'v')('E'|'e')('N'|'n') ;
HITS : ('H'|'h')('I'|'i')('T'|'t')('S'|'s') ;
IN : ('I'|'i')('N'|'n') ;
INT : ('I'|'i')('N'|'n')('T'|'t') ;
IS : ('I'|'i')('S'|'s') ;
LAST : ('L'|'l')('A'|'a')('S'|'s')('T'|'t') ;
LIKE : ('L'|'l')('I'|'i')('K'|'k')('E'|'e') ;
LIMIT : ('L'|'l')('I'|'i')('M'|'m')('I'|'i')('T'|'t') ;
LONG : ('L'|'l')('O'|'o')('N'|'n')('G'|'g') ;
MATCH : ('M'|'m')('A'|'a')('T'|'t')('C'|'c')('H'|'h') ;
NOT : ('N'|'n')('O'|'o')('T'|'t') ;
NOW : ('N'|'n')('O'|'o')('W'|'w') ;
NULL : ('N'|'n')('U'|'u')('L'|'l')('L'|'l') ;
OR : ('O'|'o')('R'|'r') ;
ORDER : ('O'|'o')('R'|'r')('D'|'d')('E'|'e')('R'|'r') ;
PARAM : ('P'|'p')('A'|'a')('R'|'r')('A'|'a')('M'|'m') ;
QUERY : ('Q'|'q')('U'|'u')('E'|'e')('R'|'r')('Y'|'y') ;
SELECT : ('S'|'s')('E'|'e')('L'|'l')('E'|'e')('C'|'c')('T'|'t') ;
SINCE : ('S'|'s')('I'|'i')('N'|'n')('C'|'c')('E'|'e') ;
STORED : ('S'|'s')('T'|'t')('O'|'o')('R'|'r')('E'|'e')('D'|'d') ;
STRING : ('S'|'s')('T'|'t')('R'|'r')('I'|'i')('N'|'n')('G'|'g') ;
TOP : ('T'|'t')('O'|'o')('P'|'p') ;
TRUE : ('T'|'t')('R'|'r')('U'|'u')('E'|'e') ;
VALUE : ('V'|'v')('A'|'a')('L'|'l')('U'|'u')('E'|'e') ;
WHERE : ('W'|'w')('H'|'h')('E'|'e')('R'|'r')('E'|'e') ;
WITH : ('W'|'w')('I'|'i')('T'|'t')('H'|'h') ;

WEEKS : ('W'|'w')('E'|'e')('E'|'e')('K'|'k')('S'|'s')? ;
DAYS : ('D'|'d')('A'|'a')('Y'|'y')('S'|'s')? ;
HOURS : ('H'|'h')('O'|'o')('U'|'u')('R'|'r')('S'|'s')? ;
MINUTES : ('M'|'m')('I'|'i')('N'|'n')('U'|'u')('T'|'t')('E'|'e')('S'|'s')? ;
MINS : ('M'|'m')('I'|'i')('N'|'n')('S'|'s')? ;
SECONDS : ('S'|'s')('E'|'e')('C'|'c')('O'|'o')('N'|'n')('D'|'d')('S'|'s')? ;
SECS : ('S'|'s')('E'|'e')('C'|'c')('S'|'s')? ;
MILLISECONDS : ('M'|'m')('I'|'i')('L'|'l')('L'|'l')('I'|'i')('S'|'s')('E'|'e')('C'|'c')('O'|'o')('N'|'n')('D'|'d')('S'|'s')? ;
MSECS : ('M'|'m')('S'|'s')('E'|'e')('C'|'c')('S'|'s')? ;

// Have to define this after the keywords?
IDENT : (ALPHA | '_') (ALPHA | DIGIT | '-' | '_')* ;
VARIABLE : '$' (ALPHA | DIGIT | '-' | '_')+ ;

WS : ( ' ' | '\t' | '\r' | '\n' )+ { $channel = HIDDEN; };

COMMENT
    : '/*' .* '*/' { $channel = HIDDEN; }
    ;

LINE_COMMENT
    : '--' ~('\n'|'\r')* '\r'? '\n' { $channel = HIDDEN; }
    ;

// ***************** parser rules:

statement returns [Object json]
    :   (   select_stmt { $json = $select_stmt.json; }
        |   describe_stmt
        )   SEMI? EOF
    ;

select_stmt returns [Object json]
@init {
    _now = System.currentTimeMillis();
    _variables = new HashSet<String>();
    boolean seenOrderBy = false;
    boolean seenLimit = false;
    boolean seenGroupBy = false;
    boolean seenBrowseBy = false;
    boolean seenFetchStored = false;
}
    :   SELECT ('*' | cols=column_name_list)
        (FROM IDENT)?
        w=where?
        given=given_clause?
        (   order_by = order_by_clause 
            { 
                if (seenOrderBy) {
                    throw new FailedPredicateException(input, "select_stmt", "ORDER BY clause can only appear once.");
                }
                else {
                    seenOrderBy = true;
                }
            }
        |   limit = limit_clause
            { 
                if (seenLimit) {
                    throw new FailedPredicateException(input, "select_stmt", "LIMIT clause can only appear once.");
                }
                else {
                    seenLimit = true;
                }
            }
        |   group_by = group_by_clause
            { 
                if (seenGroupBy) {
                    throw new FailedPredicateException(input, "select_stmt", "GROUP BY clause can only appear once.");
                }
                else {
                    seenGroupBy = true;
                }
            }
        |   browse_by = browse_by_clause
            { 
                if (seenBrowseBy) {
                    throw new FailedPredicateException(input, "select_stmt", "BROWSE BY clause can only appear once.");
                }
                else {
                    seenBrowseBy = true;
                }
            }
        |   fetch_stored = fetching_stored_clause
            { 
                if (seenFetchStored) {
                    throw new FailedPredicateException(input, "select_stmt", "FETCHING STORED clause can only appear once.");
                }
                else {
                    seenFetchStored = true;
                }
            }
        )*
        {
            JSONObject jsonObj = new JSONObject();
            JSONArray selections = new JSONArray();
            JSONObject filter = new JSONObject();
            JSONObject query = new JSONObject();

            try {
                JSONObject metaData = new JSONObject();
                if (cols == null) {
                   metaData.put("select_list", new JSONArray().put("*"));
                }
                else {
                   metaData.put("select_list", $cols.json);
                }

                if (_variables.size() > 0)
                {
                    metaData.put("variables", new JSONArray(_variables));
                }

                jsonObj.put("meta", metaData);

                if (order_by != null) {
                    jsonObj.put("sort", $order_by.json);
                }
                if (limit != null) {
                    jsonObj.put("from", $limit.offset);
                    jsonObj.put("size", $limit.count);
                }
                if (group_by != null) {
                    jsonObj.put("groupBy", $group_by.json);
                }
                if (browse_by != null) {
                    jsonObj.put("facets", $browse_by.json);
                }
                if (fetch_stored != null && $fetch_stored.val) {
                    // Default is false
                    jsonObj.put("fetchStored", $fetch_stored.val);
                }
                if (w != null) {
                    extractSelectionInfo((JSONObject) $w.json, selections, filter, query);
                    JSONObject queryPred = query.optJSONObject("query");
                    if (queryPred != null) {
                        jsonObj.put("query", queryPred);
                    }
                    if (selections.length() > 0) {
                        jsonObj.put("selections", selections);
                    }
                    JSONObject f = filter.optJSONObject("filter");
                    if (f != null) {
                        jsonObj.put("filter", f);
                    }
                }
                if (given != null) {
                    jsonObj.put("facetInit", $given.json);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "select_stmt", "JSONException: " + err.getMessage());
            }
            $json = jsonObj;
        }
        // -> ^(SELECT column_name_list IDENT where?)
    ;

describe_stmt
    :   DESCRIBE IDENT
    ;

column_name_list returns [JSONArray json]
@init {
    $json = new JSONArray();
}
    :   col=column_name { $json.put($col.text); }
        (COMMA col=column_name { $json.put($col.text); })*
        -> ^(COLUMN_LIST column_name+)
    ;

column_name
    :   IDENT
    ;

where returns [Object json]
    :   WHERE^ search_expr
        {
            $json = $search_expr.json;
        }
    ;

order_by_clause returns [Object json]
    :   ORDER BY sort_specs
        {
            $json = $sort_specs.json;
        }
    ;

sort_specs returns [Object json]
@init {
    JSONArray sortArray = new JSONArray();
}
    :   sort=sort_spec
        {
            sortArray.put($sort.json);
        }
        (COMMA sort=sort_spec   // It's OK to use variable sort again here
            {
                sortArray.put($sort.json);
            }
        )*
        {
            $json = sortArray;
        }
    ;

sort_spec returns [JSONObject json]
    :   column_name (ordering=ASC | ordering=DESC)?
        {
            $json = new JSONObject();
            try {
                if ($ordering == null) {
                    $json.put($column_name.text, "asc");
                }
                else {
                    $json.put($column_name.text, $ordering.text.toLowerCase());
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "sort_spec", "JSONException: " + err.getMessage());
            }
        }
    ;

limit_clause returns [int offset, int count]
    :   LIMIT (n1=INTEGER COMMA)? n2=INTEGER
        {
            if (n1 != null) {
                $offset = Integer.parseInt($n1.text);
            }
            else {
                $offset = DEFAULT_REQUEST_OFFSET;
            }
            $count = Integer.parseInt($n2.text);
        }
    ;

group_by_clause returns [JSONObject json]
    :   GROUP BY column_name (TOP top=INTEGER)?
        {
            $json = new JSONObject();
            try {
                JSONArray cols = new JSONArray();
                String col = $column_name.text;
                String[] facetInfo = _facetInfoMap.get(col);
                if (facetInfo != null && (facetInfo[0].equals("range") ||
                                          facetInfo[0].equals("multi") ||
                                          facetInfo[0].equals("path"))) {
                    throw new FailedPredicateException(input, 
                                                       "group_by_clause",
                                                       "Range/multi/path facet, \"" + col + "\", cannot be used in the GROUP BY clause.");
                }
                cols.put(col);
                $json.put("columns", cols);
                if (top != null) {
                    $json.put("top", Integer.parseInt(top.getText()));
                }
                else {
                    $json.put("top", DEFAULT_REQUEST_MAX_PER_GROUP);
                }                    
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "group_by_clause", "JSONException: " + err.getMessage());
            }
        }
    ;

browse_by_clause returns [JSONObject json]
@init {
    $json = new JSONObject();
}
    :   BROWSE BY f=facet_spec
        {
            try {
                $json.put($f.column, $f.spec);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "browse_by_clause", "JSONException: " + err.getMessage());
            }                    
        }
        (COMMA f=facet_spec
            {
                try {
                    $json.put($f.column, $f.spec);
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "browse_by_clause", "JSONException: " + err.getMessage());
                }
            }
        )*
    ;

facet_spec returns [String column, JSONObject spec]
@init {
    boolean expand = false;
    int minhit = DEFAULT_FACET_MINHIT;
    int max = DEFAULT_FACET_MAXHIT;
    String orderBy = "hits";
}
    :   column_name
        (
            LPAR 
            (TRUE {expand = true;} | FALSE) COMMA
            n1=INTEGER COMMA
            n2=INTEGER COMMA
            (HITS | VALUE {orderBy = "val";})
            RPAR
        )*
        {
            $column = $column_name.text;
            if (n1 != null) {
                minhit = Integer.parseInt($n1.text);
            }
            if (n2 != null) {
                max = Integer.parseInt($n2.text);
            }
            try {
                $spec = new JSONObject().put("expand", expand)
                                        .put("minhit", minhit)
                                        .put("max", max)
                                        .put("order", orderBy);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "facet_spec", "JSONException: " + err.getMessage());
            }
        }
    ;

fetching_stored_clause returns [boolean val]
@init {
    $val = true;
}
    :   FETCHING STORED
        ((TRUE | FALSE {$val = false;})
        )*
    ;

search_expr returns [Object json]
@init {
    JSONArray array = new JSONArray();
}
    :   t=term_expr { array.put($t.json); }
        (OR t=term_expr { array.put($t.json); } )*
        {
            try {
                if (array.length() == 1) {
                    $json = array.get(0);
                }
                else {
                    $json = new JSONObject().put("or", array);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "search_expr", "JSONException: " + err.getMessage());
            }
        }
        -> {array.length() > 1}? ^(OR_PRED term_expr+)
        -> term_expr
    ;

term_expr returns [Object json]
@init {
    JSONArray array = new JSONArray();
}
    :   f=factor_expr { array.put($f.json); }
        (AND f=factor_expr { array.put($f.json); } )*
        {
            try {
                JSONArray newArray = new JSONArray();
                JSONObject fieldMap = new JSONObject();
                for (int i = 0; i < array.length(); ++i) {
                    JSONObject pred = (JSONObject) array.get(i);
                    if (!"range".equals(predType(pred))) {
                        newArray.put(pred);
                    }
                    else {
                        accumulateRangePred(input, fieldMap, pred);
                    }
                }
                Iterator<String> itr = fieldMap.keys();
                while (itr.hasNext()) {
                    newArray.put(fieldMap.get(itr.next()));
                }
                if (newArray.length() == 1) {
                    $json = newArray.get(0);
                }
                else {
                    $json = new JSONObject().put("and", newArray);
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "term_expr", "JSONException: " + err.getMessage());
            }
        }
        -> { array.length() > 1}? ^(AND_PRED factor_expr+)
        -> factor_expr
    ;

factor_expr returns [Object json]
    :   predicate { $json = $predicate.json; }
    |   LPAR search_expr RPAR
        {$json = $search_expr.json;}
        -> search_expr
    ;

predicate returns [JSONObject json]
    :   in_predicate { $json = $in_predicate.json; }
    |   contains_all_predicate { $json = $contains_all_predicate.json; }
    |   equal_predicate { $json = $equal_predicate.json; }
    |   not_equal_predicate { $json = $not_equal_predicate.json; }
    |   query_predicate { $json = $query_predicate.json; }
    |   between_predicate { $json = $between_predicate.json; }
    |   range_predicate { $json = $range_predicate.json; }
    |   time_predicate { $json = $time_predicate.json; }
    |   match_predicate { $json = $match_predicate.json; }
    |   like_predicate { $json = $like_predicate.json; }
    |   null_predicate { $json = $null_predicate.json; }
    ;

in_predicate returns [JSONObject json]
    :   column_name not=NOT? IN value_list except=except_clause? predicate_props?
        {
            String col = $column_name.text;
            String[] facetInfo = _facetInfoMap.get(col);

            if (facetInfo != null && facetInfo[0].equals("range")) {
                throw new FailedPredicateException(input, 
                                                   "in_predicate",
                                                   "Range facet \"" + col + "\" cannot be used in IN predicates.");
            }
            if (!verifyFieldDataType(col, $value_list.json)) {
                throw new FailedPredicateException(input,
                                                   "in_predicate",
                                                   "Value list for IN predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            if (except != null && !verifyFieldDataType(col, $except_clause.json)) {
                throw new FailedPredicateException(input,
                                                   "in_predicate",
                                                   "EXCEPT value list for IN predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            try {
                JSONObject dict = new JSONObject();
                dict.put("operator", "or");
                if (not == null) {
                    dict.put("values", $value_list.json);
                    if (except != null) {
                        dict.put("excludes", $except_clause.json);
                    }
                    else {
                        dict.put("excludes", new JSONArray());
                    }
                }
                else {
                    dict.put("excludes", $value_list.json);
                    if (except != null) {
                        dict.put("values", $except_clause.json);
                    }
                    else {
                        dict.put("values", new JSONArray());
                    }
                }
                if (_facetInfoMap.get(col) == null) {
                    dict.put("_noOptimize", true);
                }
                $json = new JSONObject().put("terms",
                                             new JSONObject().put(col, dict));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "in_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(IN NOT? ^(column_name value_list) except_clause? predicate_props?)
    ;

contains_all_predicate returns [JSONObject json]
    :   column_name CONTAINS ALL value_list except=except_clause? predicate_props? 
        {
            String col = $column_name.text;
            String[] facetInfo = _facetInfoMap.get(col);
            if (facetInfo != null && facetInfo[0].equals("range")) {
                throw new FailedPredicateException(input, 
                                                   "contains_all_predicate", 
                                                   "Range facet column \"" + col + "\" cannot be used in CONTAINS ALL predicates.");
            }
            if (!verifyFieldDataType(col, $value_list.json)) {
                throw new FailedPredicateException(input,
                                                   "contains_all_predicate",
                                                   "Value list for CONTAINS ALL predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            if (except != null && !verifyFieldDataType(col, $except_clause.json)) {
                throw new FailedPredicateException(input,
                                                   "contains_all_predicate",
                                                   "EXCEPT value list for CONTAINS ALL predicate of facet \"" + col + "\" contains incompatible value(s).");
            }

            try {
                JSONObject dict = new JSONObject();
                dict.put("operator", "and");
                dict.put("values", $value_list.json);
                if (except != null) {
                    dict.put("excludes", $except_clause.json);
                }
                else {
                    dict.put("excludes", new JSONArray());
                }
                if (_facetInfoMap.get(col) == null) {
                    dict.put("_noOptimize", true);
                }
                $json = new JSONObject().put("terms",
                                             new JSONObject().put(col, dict));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "contains_all_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(CONTAINS ^(column_name value_list) except_clause? predicate_props?)
    ;

equal_predicate returns [JSONObject json]
    :   column_name EQUAL value props=predicate_props?
        {
            String col = $column_name.text;
            if (!verifyFieldDataType(col, $value.val)) {
                throw new FailedPredicateException(input, 
                                                   "equal_predicate", 
                                                   "Incompatible data type was found in an EQUAL predicate for column \"" + col + "\".");
            }
            try {
                String[] facetInfo = _facetInfoMap.get(col);
                if (facetInfo != null && facetInfo[0].equals("range")) {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("from", $value.val)
                                                                                      .put("to", $value.val)
                                                                                      .put("include_lower", true)
                                                                                      .put("include_upper", true)));
                }
                else if (facetInfo != null && facetInfo[0].equals("path")) {
                    JSONObject valObj = new JSONObject();
                    valObj.put("value", $value.val);
                    if (props != null) {
                        JSONObject propsJson = $props.json;
                        Iterator<String> itr = propsJson.keys();
                        while (itr.hasNext()) {
                            String key = itr.next();
                            if (key.equals("strict") || key.equals("depth")) {
                                valObj.put(key, propsJson.get(key));
                            }
                            else {
                                throw new FailedPredicateException(input,
                                                                   "equal_predicate", 
                                                                   "Unsupported property was found in an EQUAL predicate for path facet column \"" + col + "\": " + key + ".");
                            }
                        }
                    }
                    $json = new JSONObject().put("path", new JSONObject().put(col, valObj));
                }
                else {
                    JSONObject valSpec = new JSONObject().put("value", $value.val);
                    if (_facetInfoMap.get(col) == null) {
                        valSpec.put("_noOptimize", true);
                    }
                    $json = new JSONObject().put("term", 
                                                 new JSONObject().put(col, valSpec));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "equal_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(EQUAL column_name value predicate_props?)
    ;

not_equal_predicate returns [JSONObject json]
    :   column_name NOT_EQUAL value predicate_props?
        {
            String col = $column_name.text;
            if (!verifyFieldDataType(col, $value.val)) {
                throw new FailedPredicateException(input, 
                                                   "not_equal_predicate", 
                                                   "Incompatible data type was found in a NOT EQUAL predicate for column \"" + col + "\".");
            }
            try {
                String[] facetInfo = _facetInfoMap.get(col);
                if (facetInfo != null && facetInfo[0].equals("range")) {
                    JSONObject left = new JSONObject().put("range",
                                                           new JSONObject().put(col,
                                                                                new JSONObject().put("to", $value.val)
                                                                                                .put("include_upper", false)));
                    JSONObject right = new JSONObject().put("range",
                                                            new JSONObject().put(col,
                                                                                 new JSONObject().put("from", $value.val)
                                                                                                 .put("include_lower", false)));
                    $json = new JSONObject().put("or", new JSONArray().put(left).put(right));
                }
                else if (facetInfo != null && facetInfo[0].equals("path")) {
                    throw new FailedPredicateException(input, 
                                                       "not_equal_predicate",
                                                       "NOT EQUAL predicate is not supported for path facets (column \"" + col + "\").");
                }
                else {
                    JSONObject valObj = new JSONObject();
                    valObj.put("operator", "or");
                    valObj.put("values", new JSONArray());
                    valObj.put("excludes", new JSONArray().put($value.val));
                    if (_facetInfoMap.get(col) == null) {
                        valObj.put("_noOptimize", true);
                    }
                    $json = new JSONObject().put("terms",
                                                 new JSONObject().put(col, valObj));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "not_equal_predicate", "JSONException: " + err.getMessage());
            }                                         
        }
        -> ^(NOT_EQUAL column_name value predicate_props?)
    ;

query_predicate returns [JSONObject json]
    :   QUERY IS STRING_LITERAL
        {
            try {
                $json = new JSONObject().put("query",
                                             new JSONObject().put("query_string",
                                                                  new JSONObject().put("query", $STRING_LITERAL.text)));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "query_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(QUERY STRING_LITERAL)
    ;

between_predicate returns [JSONObject json]
    :   column_name not=NOT? BETWEEN val1=value AND val2=value
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "between_predicate",
                                                   "Non-range facet column \"" + col + "\" cannot be used in BETWEEN predicates.");
            }

            if (!verifyFieldDataType(col, new Object[]{$val1.val, $val2.val})) {
                throw new FailedPredicateException(input,
                                                   "between_predicate", 
                                                   "Incompatible data type was found in a BETWEEN predicate for column \"" + col + "\".");
            }

            try {
                if (not == null) {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("from", $val1.val)
                                                                                      .put("to", $val2.val)
                                                                                      .put("include_lower", true)
                                                                                      .put("include_upper", true)));
                }
                else {
                    JSONObject range1 = 
                        new JSONObject().put("range",
                                             new JSONObject().put(col,
                                                                  new JSONObject().put("to", $val1.val)
                                                                                  .put("include_upper", false)));
                    JSONObject range2 = 
                        new JSONObject().put("range",
                                             new JSONObject().put(col,
                                                                  new JSONObject().put("from", $val2.val)
                                                                                  .put("include_lower", false)));

                    $json = new JSONObject().put("or", new JSONArray().put(range1).put(range2));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "between_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^(BETWEEN NOT? $val1 $val2)
    ;

range_predicate returns [JSONObject json]
    :   column_name (op=GT | op=GTE | op=LT | op=LTE) val=value
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "range_predicate",
                                                   "Non-range facet column \"" + col + "\" cannot be used in RANGE predicates.");
            }

            if (!verifyFieldDataType(col, $val.val)) {
                throw new FailedPredicateException(input,
                                                   "range_predicate", 
                                                   "Incompatible data type was found in a RANGE predicate for column \"" + col + "\".");
            }

            try {
                if ($op.text.charAt(0) == '>') {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("from", $val.val)
                                                                                      .put("include_lower", ">=".equals($op.text))));
                }
                else {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("to", $val.val)
                                                                                      .put("include_upper", "<=".equals($op.text))));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "range_predicate", "JSONException: " + err.getMessage());
            }
        }
        -> ^($op column_name value)
    ;

time_predicate returns [JSONObject json]
    :   column_name (NOT)? IN LAST time_span
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "range_predicate",
                                                   "Non-range facet column \"" + col + "\" cannot be used in TIME predicates.");
            }

            try {
                if ($NOT == null) {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("from", $time_span.val)
                                                                                      .put("include_lower", false)));
                }
                else {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("to", $time_span.val)
                                                                                      .put("include_upper", true)));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "time_predicate", "JSONException: " + err.getMessage());
            }
        }
    |   column_name (NOT)? (since=SINCE | since=AFTER | before=BEFORE) time_expr
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new FailedPredicateException(input, 
                                                   "range_predicate",
                                                   "Non-range facet column \"" + col + "\" cannot be used in TIME predicates.");
            }

            try {
                if (since != null && $NOT == null ||
                    since == null && $NOT != null) {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("from", $time_expr.val)
                                                                                      .put("include_lower", false)));
                }
                else {
                    $json = new JSONObject().put("range",
                                                 new JSONObject().put(col,
                                                                      new JSONObject().put("to", $time_expr.val)
                                                                                      .put("include_upper", false)));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "time_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

time_span returns [long val]
    :   week=time_week_part? day=time_day_part? hour=time_hour_part? 
        minute=time_minute_part? second=time_second_part? msec=time_millisecond_part?
        {
            $val = 0;
            if (week != null) $val += $week.val;
            if (day != null) $val += $day.val;
            if (hour != null) $val += $hour.val;
            if (minute != null) $val += $minute.val;
            if (second != null) $val += $second.val;
            if (msec != null) $val += $msec.val;
            $val = _now - $val;
        }
    ;

time_week_part returns [long val]
    :   INTEGER WEEKS
        {
            $val = Integer.parseInt($INTEGER.text) * 7 * 24 * 60 * 60 * 1000L;
        }
    ;

time_day_part returns [long val]
    :   INTEGER DAYS
        {
          $val = Integer.parseInt($INTEGER.text) * 24 * 60 * 60 * 1000L;
        }
    ;

time_hour_part returns [long val]
    :   INTEGER HOURS
        {
          $val = Integer.parseInt($INTEGER.text) * 60 * 60 * 1000L;
        }
    ;

time_minute_part returns [long val]
    :   INTEGER (MINUTES | MINS)
        {
          $val = Integer.parseInt($INTEGER.text) * 60 * 1000L;
        }
    ;

time_second_part returns [long val]
    :   INTEGER (SECONDS | SECS)
        {
          $val = Integer.parseInt($INTEGER.text) * 1000L;
        }
    ;

time_millisecond_part returns [long val] 
    :   INTEGER (MILLISECONDS | MSECS)
        {
          $val = Integer.parseInt($INTEGER.text);
        }
    ;

time_expr returns [long val]
    :   time_span AGO
        {
            $val = $time_span.val;
        }
    |   date_time_string
        {
            $val = $date_time_string.val;
        }
    |   NOW
        {
            $val = _now;
        }
    ;

date_time_string returns [long val]
    :   DATE TIME?
        {
            SimpleDateFormat format;
            String dateTimeStr = $DATE.text;
            char separator = dateTimeStr.charAt(4);
            if ($TIME != null) {
                dateTimeStr = dateTimeStr + " " + $TIME.text;
            }
            int formatIdx = (separator == '-' ? 0 : 1);

            if ($TIME == null) {
                if (_format1[formatIdx] != null) {
                    format = _format1[formatIdx];
                }
                else {
                    format = _format1[formatIdx] = new SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd");
                }
            }
            else {
                if (_format2[formatIdx] != null) {
                    format = _format2[formatIdx];
                }
                else {
                    format = _format2[formatIdx] = new SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd HH:mm:ss");
                }
            }
            try {
                $val = format.parse(dateTimeStr).getTime();
                if (!dateTimeStr.equals(format.format($val))) {
                    throw new FailedPredicateException(input,
                                                       "date_time_string", 
                                                       "Date string contains invalid date/time: \"" + dateTimeStr + "\".");
                }
            }
            catch (ParseException err) {
                throw new FailedPredicateException(input,
                                                   "date_time_string", 
                                                   "ParseException happened for \"" + dateTimeStr + "\": " + 
                                                   err.getMessage() + ".");
            }
        }
    ;

match_predicate returns [JSONObject json]
    :   (NOT)? MATCH LPAR column_name_list RPAR AGAINST LPAR STRING_LITERAL RPAR
        {
            try {
                JSONArray cols = $column_name_list.json;
                for (int i = 0; i < cols.length(); ++i) {
                    String col = cols.getString(i);
                    String[] facetInfo = _facetInfoMap.get(col);
                    if (facetInfo != null && !facetInfo[1].equals("string")) {
                        throw new FailedPredicateException(input, 
                                                           "match_predicate", 
                                                           "Non-string type column \"" + col + "\" cannot be used in MATCH AGAINST predicates.");
                    }
                }
                $json = new JSONObject().put("query",
                                             new JSONObject().put("query_string",
                                                                  new JSONObject().put("fields", cols)
                                                                                  .put("query", $STRING_LITERAL.text)));
                if ($NOT != null) {
                    $json = new JSONObject().put("bool",
                                                 new JSONObject().put("must_not", $json));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "match_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

like_predicate returns [JSONObject json]
    :   column_name (NOT)? LIKE STRING_LITERAL
        {
            String col = $column_name.text;
            String[] facetInfo = _facetInfoMap.get(col);
            if (facetInfo != null && !facetInfo[1].equals("string")) {
                throw new FailedPredicateException(input, 
                                                   "match_predicate", 
                                                   "Non-string type column \"" + col + "\" cannot be used in LIKE predicates.");
            }
            String likeString = $STRING_LITERAL.text.replace('\%', '*').replace('_', '?');
            try {
                $json = new JSONObject().put("query",
                                             new JSONObject().put("wildcard",
                                                                  new JSONObject().put(col, likeString)));
                if ($NOT != null) {
                    $json = new JSONObject().put("bool",
                                                 new JSONObject().put("must_not", $json));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "like_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

null_predicate returns [JSONObject json]
    :   column_name IS (NOT)? NULL
        {
            String col = $column_name.text;
            try {
                $json = new JSONObject().put("isNull", col);
                if ($NOT != null) {
                    $json = new JSONObject().put("bool",
                                                 new JSONObject().put("must_not", $json));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "null_predicate", "JSONException: " + err.getMessage());
            }
        }
    ;

value_list returns [Object json]
@init {
    JSONArray jsonArray = new JSONArray();
}
    :   LPAR v=value
        {
            jsonArray.put($v.val);
        }
        (COMMA v=value
            {
                jsonArray.put($v.val);
            }
        )* RPAR
        {
            $json = jsonArray;
        }
    |   VARIABLE
        {
            $json = $VARIABLE.text;
            _variables.add(($VARIABLE.text).substring(1));
        }
    ;

value returns [Object val]
    :   numeric { $val = $numeric.val; }
    |   STRING_LITERAL { $val = $STRING_LITERAL.text; }
    |   TRUE { $val = true; }
    |   FALSE { $val = false; }
    |   VARIABLE
        {
            $val = $VARIABLE.text;
            _variables.add(($VARIABLE.text).substring(1));
        }
    ;

numeric returns [Object val]
    :   time_expr { $val = $time_expr.val; }
    |   INTEGER {
            try {
                $val = Long.parseLong($INTEGER.text);
            }
            catch (NumberFormatException err) {
                throw new FailedPredicateException(input, "numeric", "Hit NumberFormatException: " + err.getMessage());
            }
        }
    |   REAL {
            try {
                $val = Float.parseFloat($REAL.text);
            }
            catch (NumberFormatException err) {
                throw new FailedPredicateException(input, "numeric", "Hit NumberFormatException: " + err.getMessage());
            }
        }
    ;

except_clause returns [Object json]
    :   EXCEPT^ value_list
        {
            $json = $value_list.json;
        }
    ;
  
predicate_props returns [JSONObject json]
    :   WITH^ prop_list
        {
            $json = $prop_list.json;
        }
    ;

prop_list returns [JSONObject json]
@init {
    $json = new JSONObject();
}
    :   LPAR p=key_value_pair
        {
            try {
                $json.put($p.key, $p.value);
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "prop_list", "JSONException: " + err.getMessage());
            }
        }
        (COMMA p=key_value_pair
            {
                try {
                    $json.put($p.key, $p.value);
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "prop_list", "JSONException: " + err.getMessage());
                }
            }
        )* RPAR
        -> key_value_pair+
    ;

key_value_pair returns [String key, Object value]
    :   STRING_LITERAL COLON v=value
        {
            $key = $STRING_LITERAL.text;
            $value = $v.val;
        }
        -> ^(COLON STRING_LITERAL $v)
    ;

given_clause returns [JSONObject json]
    :   GIVEN FACET PARAM facet_param_list
        {
            $json = $facet_param_list.json;
        }
    ;

facet_param_list returns [JSONObject json]
@init {
    $json = new JSONObject();
}
    :   p=facet_param
        {
            try {
                if (!$json.has($p.facet)) {
                    $json.put($p.facet, $p.param);
                }
                else {
                    JSONObject currentParam = (JSONObject) $json.get($p.facet);
                    String paramName = (String) $p.param.keys().next();
                    currentParam.put(paramName, $p.param.get(paramName));
                }
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "facet_param_list", "JSONException: " + err.getMessage());
            }
        }
        (COMMA p=facet_param
            {
                try {
                    if (!$json.has($p.facet)) {
                        $json.put($p.facet, $p.param);
                    }
                    else {
                        JSONObject currentParam = (JSONObject) $json.get($p.facet);
                        String paramName = (String) $p.param.keys().next();
                        currentParam.put(paramName, $p.param.get(paramName));
                    }
                }
                catch (JSONException err) {
                    throw new FailedPredicateException(input, "facet_param_list", "JSONException: " + err.getMessage());
                }
            }
        )*
    ;

facet_param returns [String facet, JSONObject param]
    :   LPAR column_name COMMA STRING_LITERAL COMMA facet_param_type COMMA (val=value | valList=value_list) RPAR
        {
            $facet = $column_name.text; // XXX Check error here?
            try {
                Object valArray = (val != null) ? new JSONArray().put(val.val) : $valList.json;
                $param = new JSONObject().put($STRING_LITERAL.text,
                                              new JSONObject().put("type", $facet_param_type.paramType)
                                                              .put("values", valArray));
            }
            catch (JSONException err) {
                throw new FailedPredicateException(input, "facet_param", "JSONException: " + err.getMessage());
            }
        }                                 
    ;

facet_param_type returns [String paramType]
    :   (t=BOOLEAN | t=INT | t=LONG | t=STRING | t=BYTEARRAY | t=DOUBLE) 
        {
            $paramType = $t.text;
        }
    ;
