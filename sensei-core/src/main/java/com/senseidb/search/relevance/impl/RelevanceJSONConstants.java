package com.senseidb.search.relevance.impl;

import java.util.Map;
import java.util.HashMap;

public class RelevanceJSONConstants
{
  /* JSON keywords*/

  // (1) json keys;
  public static final String           KW_MODEL                = "model";
  public static final String           KW_PREDEFINED           = "predefined_model";
  public static final String           KW_KEY                  = "key";
  public static final String           KW_VALUE                = "value";
  public static final String           KW_VALUES               = "values";
  public static final String           KW_VARIABLES            = "variables";
  public static final String           KW_FACETS               = "facets";
  public static final String           KW_FUNC_PARAMETERS      = "function_params";
  public static final String           KW_FUNCTION             = "function";
  public static final String           KW_SAVE_AS              = "save_as";
  public static final String           KW_NAME_AS              = "name";
  public static final String           KW_OVERWRITE            = "overwrite";


  // (2) supported types in json:
  // set type: [set_int, set_float, set_string, set_double, set_long]
  public static final String           KW_TYPE_SET_INT         = "set_int";
  public static final String           KW_TYPE_SET_FLOAT       = "set_float";
  public static final String           KW_TYPE_SET_STRING      = "set_string";
  public static final String           KW_TYPE_SET_DOUBLE      = "set_double";
  public static final String           KW_TYPE_SET_LONG        = "set_long";

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
  
  // custom type:
  public static final String           KW_TYPE_CUSTOM          = "custom_obj";

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
  
  // activity engine facet type support: [aint]
  public static final String           KW_TYPE_FACET_A_INT     = "aint";

  
  // constant type:
  public static final String           KW_INNER_SCORE          = "_INNER_SCORE";
  public static final String           KW_NOW                  = "_NOW";
  public static final String           KW_RANDOM               = "_RANDOM";

  /* Type Numbers 
   * 
   * The numbers below are used to in an ordered way, so adding new types should be careful.
   * */

  // (1) special type number (inner score, now, custom_obj, etc.);
  public static final int TYPENUMBER_INNER_SCORE       =  0;
  public static final int TYPENUMBER_NOW               =  1;
  
  public static final int TYPENUMBER_CUSTOM_OBJ        =  2;

  // (2) general type numbers:
  public static final int TYPENUMBER_INT               = 10;
  public static final int TYPENUMBER_LONG              = 20;
  public static final int TYPENUMBER_DOUBLE            = 30;
  public static final int TYPENUMBER_FLOAT             = 40;
  public static final int TYPENUMBER_BOOLEAN           = 50;
  public static final int TYPENUMBER_STRING            = 60;

  public static final int TYPENUMBER_SET               =  7;
  public static final int TYPENUMBER_MAP               =  8;

  public static final int TYPENUMBER_SET_INT           = 70;
  public static final int TYPENUMBER_SET_LONG          = 71;
  public static final int TYPENUMBER_SET_DOUBLE        = 72;
  public static final int TYPENUMBER_SET_FLOAT         = 73;
  public static final int TYPENUMBER_SET_STRING        = 75;

  public static final int TYPENUMBER_MAP_INT_INT       = 80;
  public static final int TYPENUMBER_MAP_INT_LONG      = 81;
  public static final int TYPENUMBER_MAP_INT_DOUBLE    = 82;
  public static final int TYPENUMBER_MAP_INT_FLOAT     = 83;
  public static final int TYPENUMBER_MAP_INT_STRING    = 84;
  public static final int TYPENUMBER_MAP_STRING_INT    = 85;
  public static final int TYPENUMBER_MAP_STRING_LONG   = 86;
  public static final int TYPENUMBER_MAP_STRING_DOUBLE = 87;
  public static final int TYPENUMBER_MAP_STRING_FLOAT  = 88;
  public static final int TYPENUMBER_MAP_STRING_STRING = 89;

  // (3) facet type numbers;
  public static final int TYPENUMBER_FACET_INT         = 100;
  public static final int TYPENUMBER_FACET_LONG        = 110;
  public static final int TYPENUMBER_FACET_DOUBLE      = 120;
  public static final int TYPENUMBER_FACET_FLOAT       = 130;
  public static final int TYPENUMBER_FACET_SHORT       = 140;
  public static final int TYPENUMBER_FACET_STRING      = 150;

  // (4) multi-facet type numbers;
  public static final int TYPENUMBER_FACET_M_INT       = 200;
  public static final int TYPENUMBER_FACET_M_LONG      = 210;
  public static final int TYPENUMBER_FACET_M_DOUBLE    = 220;
  public static final int TYPENUMBER_FACET_M_FLOAT     = 230;
  public static final int TYPENUMBER_FACET_M_SHORT     = 240;
  public static final int TYPENUMBER_FACET_M_STRING    = 250;

  // (5) weighted multi-facet type numbers;
  public static final int TYPENUMBER_FACET_WM_INT      = 300;
  public static final int TYPENUMBER_FACET_WM_LONG     = 310;
  public static final int TYPENUMBER_FACET_WM_DOUBLE   = 320;
  public static final int TYPENUMBER_FACET_WM_FLOAT    = 330;
  public static final int TYPENUMBER_FACET_WM_SHORT    = 340;
  public static final int TYPENUMBER_FACET_WM_STRING   = 350;
  
  // (6) activity engine facet type numbers;
  public static final int TYPENUMBER_FACET_A_INT       = 400;

  // A map from facet type names to an integer array whose first element
  // is the facet type number and the second element indicates whether
  // the facet is a multi-value facet (1) or normal one (0) or an activity facet (2).
  public static Map<String, Integer[]> FACET_INFO_MAP = new HashMap<String, Integer[]>();

  // A map from variable type names to variable type numbers.
  public static Map<String, Integer> VARIABLE_INFO_MAP = new HashMap<String, Integer>();

  static
  {
    FACET_INFO_MAP.put(KW_TYPE_FACET_INT,       new Integer[]{TYPENUMBER_FACET_INT,       0});
    FACET_INFO_MAP.put(KW_TYPE_FACET_SHORT,     new Integer[]{TYPENUMBER_FACET_SHORT,     0});
    FACET_INFO_MAP.put(KW_TYPE_FACET_DOUBLE,    new Integer[]{TYPENUMBER_FACET_DOUBLE,    0});
    FACET_INFO_MAP.put(KW_TYPE_FACET_FLOAT,     new Integer[]{TYPENUMBER_FACET_FLOAT,     0});
    FACET_INFO_MAP.put(KW_TYPE_FACET_LONG,      new Integer[]{TYPENUMBER_FACET_LONG,      0});
    FACET_INFO_MAP.put(KW_TYPE_FACET_STRING,    new Integer[]{TYPENUMBER_FACET_STRING,    0});
    FACET_INFO_MAP.put(KW_TYPE_FACET_M_INT,     new Integer[]{TYPENUMBER_FACET_M_INT,     1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_M_SHORT,   new Integer[]{TYPENUMBER_FACET_M_SHORT,   1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_M_DOUBLE,  new Integer[]{TYPENUMBER_FACET_M_DOUBLE,  1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_M_FLOAT,   new Integer[]{TYPENUMBER_FACET_M_FLOAT,   1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_M_LONG,    new Integer[]{TYPENUMBER_FACET_M_LONG,    1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_M_STRING,  new Integer[]{TYPENUMBER_FACET_M_STRING,  1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_WM_INT,    new Integer[]{TYPENUMBER_FACET_WM_INT,    1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_WM_SHORT,  new Integer[]{TYPENUMBER_FACET_WM_SHORT,  1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_WM_DOUBLE, new Integer[]{TYPENUMBER_FACET_WM_DOUBLE, 1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_WM_FLOAT,  new Integer[]{TYPENUMBER_FACET_WM_FLOAT,  1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_WM_LONG,   new Integer[]{TYPENUMBER_FACET_WM_LONG,   1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_WM_STRING, new Integer[]{TYPENUMBER_FACET_WM_STRING, 1});
    FACET_INFO_MAP.put(KW_TYPE_FACET_A_INT,     new Integer[]{TYPENUMBER_FACET_A_INT,     2});

    VARIABLE_INFO_MAP.put(KW_TYPE_INT,                  TYPENUMBER_INT);
    VARIABLE_INFO_MAP.put(KW_TYPE_LONG,                 TYPENUMBER_LONG);
    VARIABLE_INFO_MAP.put(KW_TYPE_DOUBLE,               TYPENUMBER_DOUBLE);
    VARIABLE_INFO_MAP.put(KW_TYPE_FLOAT,                TYPENUMBER_FLOAT);
    VARIABLE_INFO_MAP.put(KW_TYPE_BOOL,                 TYPENUMBER_BOOLEAN);
    VARIABLE_INFO_MAP.put(KW_TYPE_STRING,               TYPENUMBER_STRING);
    VARIABLE_INFO_MAP.put(KW_TYPE_SET_INT,              TYPENUMBER_SET_INT);
    VARIABLE_INFO_MAP.put(KW_TYPE_SET_LONG,             TYPENUMBER_SET_LONG);
    VARIABLE_INFO_MAP.put(KW_TYPE_SET_DOUBLE,           TYPENUMBER_SET_DOUBLE);
    VARIABLE_INFO_MAP.put(KW_TYPE_SET_FLOAT,            TYPENUMBER_SET_FLOAT);
    VARIABLE_INFO_MAP.put(KW_TYPE_SET_STRING,           TYPENUMBER_SET_STRING);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_INT_INT,          TYPENUMBER_MAP_INT_INT);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_INT_LONG,         TYPENUMBER_MAP_INT_LONG);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_INT_DOUBLE,       TYPENUMBER_MAP_INT_DOUBLE);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_INT_FLOAT,        TYPENUMBER_MAP_INT_FLOAT);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_INT_STRING,       TYPENUMBER_MAP_INT_STRING);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_STRING_INT,       TYPENUMBER_MAP_STRING_INT);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_STRING_LONG,      TYPENUMBER_MAP_STRING_LONG);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_STRING_DOUBLE,    TYPENUMBER_MAP_STRING_DOUBLE);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_STRING_FLOAT,     TYPENUMBER_MAP_STRING_FLOAT);
    VARIABLE_INFO_MAP.put(KW_TYPE_MAP_STRING_STRING,    TYPENUMBER_MAP_STRING_STRING);
    VARIABLE_INFO_MAP.put(KW_TYPE_CUSTOM,               TYPENUMBER_CUSTOM_OBJ);
  }

}
