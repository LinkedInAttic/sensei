package com.senseidb.search.relevance;

public class JSONConstants
{
  /* JSON keywords*/
  
  // (1) json keys;
  public static final String           KW_MODEL                = "model";
  public static final String           KW_PREDEFINED           = "predefined-model";
  public static final String           KW_VALUES               = "values";
  public static final String           KW_VARIABLES            = "variables";
  public static final String           KW_FACETS               = "facets";
  public static final String           KW_FUNC_PARAMETERS      = "function_params";
  public static final String           KW_FUNCTION             = "function";

  // (2) supported types in json:
  // set type: [set_int, set_float, set_string, set_double, set_long]
  public static final String           KW_TYPE_SET_INT         = "set_int";
  public static final String           KW_TYPE_SET_FLOAT       = "set_float";
  public static final String           KW_TYPE_SET_STRING      = "set_string";
  public static final String           KW_TYPE_SET_DOUBLE      = "set_double";
  public static final String           KW_TYPE_SET_LONG        = "set_long";
  
  // map type: [map_int_int, map_int_float, map_int_double, map_int_long, map_int_string]
  public static final String           KW_TYPE_MAP_HEAD            = "map_"; 
  
  public static final String           KW_TYPE_MAP_INT_INT         = "map_int_int";
  public static final String           KW_TYPE_MAP_INT_FLOAT       = "map_int_float";
  public static final String           KW_TYPE_MAP_INT_STRING      = "map_int_string";
  public static final String           KW_TYPE_MAP_INT_DOUBLE      = "map_int_double";
  public static final String           KW_TYPE_MAP_INT_LONG        = "map_int_long";

  public static final String           KW_TYPE_MAP_STRING_INT         = "map_string_int";
  public static final String           KW_TYPE_MAP_STRING_FLOAT       = "map_string_float";
  public static final String           KW_TYPE_MAP_STRING_STRING      = "map_string_string";
  public static final String           KW_TYPE_MAP_STRING_DOUBLE      = "map_string_double";
  public static final String           KW_TYPE_MAP_STRING_LONG        = "map_string_long";
  
  // normal type: [int, double, float, long, bool, string]
  public static final String           KW_TYPE_INT             = "int";
  public static final String           KW_TYPE_FLOAT           = "float";
  public static final String           KW_TYPE_STRING          = "string";
  public static final String           KW_TYPE_DOUBLE          = "double";
  public static final String           KW_TYPE_LONG            = "long";
  public static final String           KW_TYPE_BOOL            = "bool";

  // facet type support: [double, float, int, long, short, string]
  public static final String           KW_TYPE_FACET_INT       = "int";
  public static final String           KW_TYPE_FACET_FLOAT     = "float";
  public static final String           KW_TYPE_FACET_STRING    = "string";
  public static final String           KW_TYPE_FACET_DOUBLE    = "double";
  public static final String           KW_TYPE_FACET_LONG      = "long";
  public static final String           KW_TYPE_FACET_SHORT     = "short";

  // multi-facet type support: [mdouble, mfloat, mint, mlong, mshort, mstring]
  public static final String           KW_TYPE_FACET_M_INT       = "mint";
  public static final String           KW_TYPE_FACET_M_FLOAT     = "mfloat";
  public static final String           KW_TYPE_FACET_M_STRING    = "mstring";
  public static final String           KW_TYPE_FACET_M_DOUBLE    = "mdouble";
  public static final String           KW_TYPE_FACET_M_LONG      = "mlong";
  public static final String           KW_TYPE_FACET_M_SHORT     = "mshort";
  

  // weighted multi-facet type support: [mdouble, mfloat, mint, mlong, mshort, mstring]
  public static final String           KW_TYPE_FACET_WM_INT       = "wmint";
  public static final String           KW_TYPE_FACET_WM_FLOAT     = "wmfloat";
  public static final String           KW_TYPE_FACET_WM_STRING    = "wmstring";
  public static final String           KW_TYPE_FACET_WM_DOUBLE    = "wmdouble";
  public static final String           KW_TYPE_FACET_WM_LONG      = "wmlong";
  public static final String           KW_TYPE_FACET_WM_SHORT     = "wmshort";

  // constant type:
  public static final String           KW_INNER_SCORE          = "_INNER_SCORE";
  public static final String           KW_NOW                  = "_NOW";
  
  
  /* Type Strings; */
  
  // (1) inner score type;
  public static final String                 TYPE_INNER_SCORE  = "INNER_SCORE";  //actually a float value;
  
  // (2) general types:
  public static final String                 TYPE_INT          = "INT";
  public static final String                 TYPE_LONG         = "LONG";
  public static final String                 TYPE_DOUBLE       = "DOUBLE";
  public static final String                 TYPE_FLOAT        = "FLOAT";
  public static final String                 TYPE_BOOLEAN      = "BOOLEAN";
  public static final String                 TYPE_STRING       = "STRING";

  // hashset container types:
  public static final String                 TYPE_SET_INT          = "SET_INT";
  public static final String                 TYPE_SET_LONG         = "SET_LONG";
  public static final String                 TYPE_SET_DOUBLE       = "SET_DOUBLE";
  public static final String                 TYPE_SET_FLOAT        = "SET_FLOAT";
  public static final String                 TYPE_SET_STRING       = "SET_STRING";
  
  public static final String                 TYPE_SET_HEAD         = "SET";
  
  // hashmap container types:
  public static final String                 TYPE_MAP_INT_INT          = "MAP_INT_INT";
  public static final String                 TYPE_MAP_INT_LONG         = "MAP_INT_LONG";
  public static final String                 TYPE_MAP_INT_DOUBLE       = "MAP_INT_DOUBLE";
  public static final String                 TYPE_MAP_INT_FLOAT        = "MAP_INT_FLOAT";
  public static final String                 TYPE_MAP_INT_STRING       = "MAP_INT_STRING";
  public static final String                 TYPE_MAP_STRING_INT          = "MAP_STRING_INT";
  public static final String                 TYPE_MAP_STRING_LONG         = "MAP_STRING_LONG";
  public static final String                 TYPE_MAP_STRING_DOUBLE       = "MAP_STRING_DOUBLE";
  public static final String                 TYPE_MAP_STRING_FLOAT        = "MAP_STRING_FLOAT";
  public static final String                 TYPE_MAP_STRING_STRING       = "MAP_STRING_STRING";
  
  public static final String                 TYPE_MAP_HEAD         = "MAP";

  // (3) facet types:
  public static final String                 TYPE_FACET_INT    = "FACET_INT";
  public static final String                 TYPE_FACET_LONG   = "FACET_LONG";
  public static final String                 TYPE_FACET_DOUBLE = "FACET_DOUBLE";
  public static final String                 TYPE_FACET_FLOAT  = "FACET_FLOAT";
  public static final String                 TYPE_FACET_SHORT  = "FACET_SHORT";
  public static final String                 TYPE_FACET_STRING = "FACET_STRING";
  
  // (4) multi-facet types:
  public static final String                 TYPE_FACET_M_INT    = "FACET_M_INT";
  public static final String                 TYPE_FACET_M_LONG   = "FACET_M_LONG";
  public static final String                 TYPE_FACET_M_DOUBLE = "FACET_M_DOUBLE";
  public static final String                 TYPE_FACET_M_FLOAT  = "FACET_M_FLOAT";
  public static final String                 TYPE_FACET_M_SHORT  = "FACET_M_SHORT";
  public static final String                 TYPE_FACET_M_STRING = "FACET_M_STRING";
  
  // (4) weighted multi-facet types:
  public static final String                 TYPE_FACET_WM_INT    = "FACET_WM_INT";
  public static final String                 TYPE_FACET_WM_LONG   = "FACET_WM_LONG";
  public static final String                 TYPE_FACET_WM_DOUBLE = "FACET_WM_DOUBLE";
  public static final String                 TYPE_FACET_WM_FLOAT  = "FACET_WM_FLOAT";
  public static final String                 TYPE_FACET_WM_SHORT  = "FACET_WM_SHORT";
  public static final String                 TYPE_FACET_WM_STRING = "FACET_WM_STRING";
  
  public static final String                 TYPE_FACET_HEAD    = "FACET";
  public static final String                 TYPE_M_FACET_HEAD  = "FACET_M";
  public static final String                 TYPE_WM_FACET_HEAD = "FACET_WM";
  
  
  /* Type Numbers */
  
  // (1) inner score type number;
  public static final int                 TYPENUMBER_INNER_SCORE  = 0;
  
  // (2) general type numbers:
  public static final int                 TYPENUMBER_INT          = 1;
  public static final int                 TYPENUMBER_LONG         = 2;
  public static final int                 TYPENUMBER_DOUBLE       = 3;
  public static final int                 TYPENUMBER_FLOAT        = 4;
  public static final int                 TYPENUMBER_BOOLEAN      = 5;
  public static final int                 TYPENUMBER_STRING       = 6;  
  
  public static final int                 TYPENUMBER_SET          = 7;
  public static final int                 TYPENUMBER_MAP          = 8;
  
  // (3) facet type numbers;
  public static final int                 TYPENUMBER_FACET_INT    = 10;
  public static final int                 TYPENUMBER_FACET_LONG   = 11;
  public static final int                 TYPENUMBER_FACET_DOUBLE = 12;
  public static final int                 TYPENUMBER_FACET_FLOAT  = 13;
  public static final int                 TYPENUMBER_FACET_SHORT  = 14;
  public static final int                 TYPENUMBER_FACET_STRING = 15;
  
  // (4) multi-facet type numbers;
  public static final int                 TYPENUMBER_FACET_M_INT    = 20;
  public static final int                 TYPENUMBER_FACET_M_LONG   = 21;
  public static final int                 TYPENUMBER_FACET_M_DOUBLE = 22;
  public static final int                 TYPENUMBER_FACET_M_FLOAT  = 23;
  public static final int                 TYPENUMBER_FACET_M_SHORT  = 24;
  public static final int                 TYPENUMBER_FACET_M_STRING = 25;
  
  // (5) weighted multi-facet type numbers;
  public static final int                 TYPENUMBER_FACET_WM_INT    = 30;
  public static final int                 TYPENUMBER_FACET_WM_LONG   = 31;
  public static final int                 TYPENUMBER_FACET_WM_DOUBLE = 32;
  public static final int                 TYPENUMBER_FACET_WM_FLOAT  = 33;
  public static final int                 TYPENUMBER_FACET_WM_SHORT  = 34;
  public static final int                 TYPENUMBER_FACET_WM_STRING = 35;
  
}
