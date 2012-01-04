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

// As the generated lexer will reside in com.sensei.bql.parsers package,
// we have to add package declaration on top of it
@lexer::header {
package com.sensei.bql.parsers;
}

// As the generated parser will reside in com.sensei.bql.parsers
// package, we have to add package declaration on top of it
@parser::header {
package com.sensei.bql.parsers;

import java.util.Iterator;
import java.util.Map;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
}

@parser::members {
    Map<String, String[]> _facetInfoMap;

    public BQLParser(TokenStream input, Map<String, String[]> facetInfoMap) {
        this(input);
        _facetInfoMap = facetInfoMap;
    }

    private String predType(JSONObject pred) {
        return (String) pred.keys().next();
    }

    private String predField(JSONObject pred) throws JSONException {
        String type = (String) pred.keys().next();
        JSONObject fieldSpec = pred.getJSONObject(type);
        return (String) fieldSpec.keys().next();
    }

    private boolean verifyFacetType(final String field, final String expectedType)
    {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            return (expectedType.equals(facetInfo[0]));
        }
        else {
            return true;
        }
    }

    private boolean verifyValueType(Object value, final String columnType)
    {
        if (columnType.equals("int") || columnType.equals("short")) {
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
        else {
            return false;
        }
    }

    private boolean verifyFieldDataType(final String field, Object value)
    {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            return verifyValueType(value, facetInfo[1]);
        }
        else {
            // Field is not a facet
            return true;
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

    private boolean verifyFieldDataType(final String field, JSONArray values)
    {
        String[] facetInfo = _facetInfoMap.get(field);
        try {
            if (facetInfo != null) {
                String columnType = facetInfo[1];
                for (int i = 0; i < values.length(); ++i) {
                    if (!verifyValueType(values.get(i), columnType)) {
                        return false;
                    }
                }
            }
            return true;
        }
        catch (JSONException err) {
            return false;
        }
    }

    private void extractSelectionInfo(JSONObject where,
                                      JSONArray selections,
                                      JSONObject filter,
                                      JSONObject query) throws JSONException {

        if (where.has("query")) {
            query.put("query", where.get("query"));
        }
        else if (where.has("and")) {
            JSONArray preds = (JSONArray) where.get("and");
            JSONArray filter_list = new JSONArray();
            for (int i = 0; i < preds.length(); ++i) {
                JSONObject pred = preds.getJSONObject(i);
                if (pred.has("query")) {
                    if (!query.has("query")) {
                        query.put("query", pred.get("query"));
                    }
                    else {
                        filter_list.put(pred);
                    }
                }
                else if (pred.has("and") || pred.has("or")) {
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
        else if (where.has("or")) {
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
                System.out.println(">>> in getmax, value2 = " + value2 + ", include2 = " + include2);
                value = value2;
                include = include2;
            }
        }
        return new Object[]{value, include2};
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
        return new Object[]{value, include1};
    }

    private void accumulateRangePred(JSONObject fieldMap, JSONObject pred) throws JSONException, RecognitionException
    {
        String field = predField(pred);
        if (!fieldMap.has(field)) {
            fieldMap.put(field, pred);
            return;
        }
        JSONObject oldRange = (JSONObject) fieldMap.get(field);
        JSONObject oldSpec = (JSONObject) ((JSONObject) oldRange.get("range")).get(field);
        Object oldFrom = (oldSpec.has("from") ? oldSpec.get("from") : null);
        Object oldTo = (oldSpec.has("to") ? oldSpec.get("to") : null);
        Boolean oldIncludeLower = (oldSpec.has("include_lower") ? (Boolean) oldSpec.get("include_lower") : false);
        Boolean oldIncludeUpper = (oldSpec.has("include_upper") ? (Boolean) oldSpec.get("include_upper") : false);
        
        JSONObject curSpec = (JSONObject) ((JSONObject) pred.get("range")).get(field);
        Object curFrom = (curSpec.has("from") ? curSpec.get("from") : null);
        Object curTo = (curSpec.has("to") ? curSpec.get("to") : null);
        Boolean curIncludeLower = (curSpec.has("include_lower") ? (Boolean) curSpec.get("include_lower") : false);
        Boolean curIncludeUpper = (curSpec.has("include_upper") ? (Boolean) curSpec.get("include_upper") : false);

        Object[] result = getMax(oldFrom, oldIncludeLower, curFrom, curIncludeLower);
        Object newFrom = result[0];
        Boolean newIncludeLower = (Boolean) result[1];
        result = getMin(oldTo, oldIncludeUpper, curTo, curIncludeUpper);
        Object newTo = result[0];
        Boolean newIncludeUpper = (Boolean) result[1];

        // XXX Check inconsistency
        
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
    catch (RecognitionException e) {
        throw e;
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

// XXX have to define this after the keywords?
IDENT : ALPHA (ALPHA | DIGIT | '-' )* ;

WS : ( ' ' | '\t' | '\r' | '\n' )+ { $channel = HIDDEN; };


// ***************** parser rules:

statement returns [Object json]
    :   (   select_stmt { $json = $select_stmt.json; }
        |   describe_stmt
        )   SEMI?
    ;

select_stmt returns [Object json]
    :   SELECT ('*' | column_name_list)
        (FROM IDENT)?
        w=where?
        given=given_clause?
        (   order_by = order_by_clause
        |   limit = limit_clause
        |   group_by = group_by_clause
        |   browse_by = browse_by_clause
        |   fetch_stored = fetching_stored_clause
        )*
        {
            JSONObject jsonObj = new JSONObject();
            JSONArray selections = new JSONArray();
            JSONObject filter = new JSONObject();
            JSONObject query = new JSONObject();

            try {
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
                    if (query.has("query")) {
                        jsonObj.put("query", query.get("query"));
                    }
                    if (selections.length() > 0) {
                        jsonObj.put("selections", selections);
                    }
                    if (filter.has("filter")) {
                        jsonObj.put("filter", filter.get("filter"));
                    }
                }
                if (given != null) {
                    jsonObj.put("facetInit", $given.json);
                }
            }
            catch (JSONException err) {
                // XXX
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
                // XXX
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
                $offset = 0;
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
                cols.put($column_name.text);
                $json.put("columns", cols);
                if (top != null) {
                    $json.put("top", Integer.parseInt(top.getText()));
                }
            }
            catch (JSONException err) {
                // XXX
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
                // XXX
            }                    
        }
        (COMMA f=facet_spec
            {
                try {
                    $json.put($f.column, $f.spec);
                }
                catch (JSONException err) {
                    // XXX
                }
            }
        )*
    ;

facet_spec returns [String column, JSONObject spec]
@init {
    boolean expand = false;
    int minhit = 0;
    int max = 10;
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
                // XXX
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
                // XXX
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
                        accumulateRangePred(fieldMap, pred);
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
                // XXX
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
 // |   time_predicate { $json = $time_predicate.json; }
    |   match_predicate { $json = $match_predicate.json; }
 // |   like_predicate { $json = $like_predicate.json; }
    ;

in_predicate returns [JSONObject json]
    :   column_name not=NOT? IN value_list except=except_clause? predicate_props?
        {
            String col = $column_name.text;
            String[] facetInfo = _facetInfoMap.get(col);

            if (facetInfo != null && facetInfo[0].equals("range")) {
                throw new RecognitionException();
            }
            if (!verifyFieldDataType(col, $value_list.json)) {
                throw new RecognitionException();
            }

            if (except != null && !verifyFieldDataType(col, $except_clause.json)) {
                throw new RecognitionException();
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
                $json = new JSONObject().put("terms",
                                             new JSONObject().put(col, dict));
            }
            catch (JSONException err) {
                // XXX
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
                throw new RecognitionException();
            }
            if (!verifyFieldDataType(col, $value_list.json)) {
                throw new RecognitionException();
            }

            if (except != null && !verifyFieldDataType(col, $except_clause.json)) {
                throw new RecognitionException();
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
                $json = new JSONObject().put("terms",
                                             new JSONObject().put(col, dict));
            }
            catch (JSONException err) {
                // XXX
            }
        }
        -> ^(CONTAINS ^(column_name value_list) except_clause? predicate_props?)
    ;

equal_predicate returns [JSONObject json]
    :   column_name EQUAL value props=predicate_props?
        {
            String col = $column_name.text;
            if (!verifyFieldDataType(col, $value.val)) {
                throw new RecognitionException();
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
                                throw new RecognitionException();
                            }
                        }
                    }
                    $json = new JSONObject().put("path", new JSONObject().put(col, valObj));
                }
                else {
                    $json = new JSONObject().put("term", 
                                                 new JSONObject().put(col, 
                                                                      new JSONObject().put("value", $value.val)));
                }
            }
            catch (JSONException err) {
                // XXX
            }
        }
        -> ^(EQUAL column_name value predicate_props?)
    ;

not_equal_predicate returns [JSONObject json]
    :   column_name NOT_EQUAL value predicate_props?
        {
            String col = $column_name.text;
            if (!verifyFieldDataType(col, $value.val)) {
                throw new RecognitionException();
            }
            try {
                JSONObject dict = new JSONObject();
                dict.put("operator", "or");
                dict.put("values", new JSONArray());
                dict.put("excludes", new JSONArray().put($value.val));
                $json = new JSONObject().put("terms",
                                             new JSONObject().put(col, dict));
            }
            catch (JSONException err) {
                // XXX
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
                // XXX
            }
        }
        -> ^(QUERY STRING_LITERAL)
    ;

between_predicate returns [JSONObject json]
    :   column_name not=NOT? BETWEEN val1=value AND val2=value
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new RecognitionException();
            }

            if (!verifyFieldDataType(col, new Object[]{val1, val2})) {
                throw new RecognitionException();
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
                // XXX
            }
        }
        -> ^(BETWEEN NOT? $val1 $val2)
    ;

range_predicate returns [JSONObject json]
    :   column_name (op=GT | op=GTE | op=LT | op=LTE) val=value
        {
            String col = $column_name.text;
            if (!verifyFacetType(col, "range")) {
                throw new RecognitionException();
            }

            if (!verifyFieldDataType(col, $val.val)) {
                throw new RecognitionException();
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
                // XXX
            }
        }
        -> ^($op column_name value)
    ;

match_predicate returns [JSONObject json]
    :   MATCH LPAR column_name_list RPAR AGAINST LPAR STRING_LITERAL RPAR
        {
            try {
                $json = new JSONObject().put("query",
                                             new JSONObject().put("query_string",
                                                                  new JSONObject().put("fields", $column_name_list.json)
                                                                                  .put("query", $STRING_LITERAL.text)));
            }
            catch (JSONException err) {
                // XXX
            }
        }
    ;

value_list returns [JSONArray json]
@init {
    $json = new JSONArray();
}
    :   LPAR v=value
        {
            $json.put($v.val);
        }
        (COMMA v=value
            {
                $json.put($v.val);
            }
        )* RPAR -> value+
    ;

value returns [Object val]
    :   numeric { $val = $numeric.val; }
    |   STRING_LITERAL { $val = $STRING_LITERAL.text; }
    |   TRUE { $val = true; }
    |   FALSE { $val = false; }
    ;

numeric returns [Object val]
    :   INTEGER { $val = Integer.parseInt($INTEGER.text); }
    |   REAL { $val = Float.parseFloat($REAL.text); }
    ;

except_clause returns [JSONArray json]
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
                // XXX
            }
        }
        (COMMA p=key_value_pair
            {
                try {
                    $json.put($p.key, $p.value);
                }
                catch (JSONException err) {
                    // XXX
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
                // XXX
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
                    // XXX
                }
            }
        )*
    ;

facet_param returns [String facet, JSONObject param]
    :   LPAR column_name COMMA STRING_LITERAL COMMA facet_param_type COMMA value RPAR
        {
            $facet = $column_name.text; // XXX Check error here?
            try {
                $param = new JSONObject().put($STRING_LITERAL.text,
                                              new JSONObject().put("type", $facet_param_type.paramType)
                                                              .put("values", new JSONArray().put($value.val)));
            }
            catch (JSONException err) {
                // XXX
            }
        }                                 
    ;

facet_param_type returns [String paramType]
    :   (t=BOOLEAN | t=INT | t=LONG | t=STRING | t=BYTEARRAY | t=DOUBLE) 
        {
            $paramType = $t.text;
        }
    ;
