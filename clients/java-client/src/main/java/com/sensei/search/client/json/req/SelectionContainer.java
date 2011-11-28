package com.sensei.search.client.json.req;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;



public class SelectionContainer {
    private Term term;
    private Terms terms;
    private Path path;
    private Range range;
    private JSONObject custom;
    
    
    public static class Term implements Selection {
        private String field;
        private String value;
        public Term(String field, String value) {
            super();
            this.field = field;
            this.value = value;
        }
        public Term() {
           
        }
    }
    public static class Terms implements Selection {
        String field;
        List<String> values; 
        List<String> excludes; 
        SelectionContainer.Operator op; 
        public Terms() {
            
        }
        public Terms(String field, List<String> values, List<String> excludes, Operator op) {
            super();
            this.field = field;
            this.values = values;
            this.excludes = excludes;
            this.op = op;
        }
        
    }
    public static class Path implements Selection {
        private String field;
        private String value;
        private boolean strict; 
        private int depth;
        public Path(String field, String value, boolean strict, int depth) {
            super();
            this.field = field;
            this.value = value;
            this.strict = strict;
            this.depth = depth;
        }

        public Path() {
           
        }
    }
    public static class Range implements Selection {
        private String field;
        private String upper;
        private String lower;
       public Range() {
       }
    public Range(String field, String upper, String lower) {
        super();
        this.field = field;
        this.upper = upper;
        this.lower = lower;
    }
       
    }
    public static enum Operator{and, or;}
    public static SelectionContainer term(String field, String value) {
        SelectionContainer selection = new SelectionContainer();
        selection.term = new Term(field, value);
        return selection;
    }
    public static SelectionContainer terms(String field, List<String> values, List<String> excludes, Operator op) {
        SelectionContainer selection = new SelectionContainer();
        selection.terms = new Terms(field, values,excludes, op);
        return selection;
    }
    public static SelectionContainer range(String field, String upper, String lower) {
        SelectionContainer selection = new SelectionContainer();
        selection.range = new Range(field, upper, lower);
        return selection;
    }
    public static SelectionContainer path(String field, String value, boolean strict, int depth) {
        SelectionContainer selection = new SelectionContainer();
        selection.path = new Path(field, value, strict, depth);
        return selection;
    }
    public static SelectionContainer custom(JSONObject custom) {
        SelectionContainer selection = new SelectionContainer();
        selection.custom = custom;
        return selection;
    }
    public Selection get() {
        if (term != null) {
            return term;
        }
        if (terms != null) {
            return terms;
        }
        if (path != null) {
            return path;
        }
        if (range != null) {
            return range;
        }        
        return null;
    }
    public JSONObject getCustom() {        
        return custom;
    }
}