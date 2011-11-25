package com.sensei.search.req.json.domain;

import java.util.Arrays;
import java.util.List;
import java.util.Map;



public class Selection {
    List<String> values; 
    List<String> excludes; 
    Selection.Operator op; 
    Map<String,String> props;
    public static enum Operator{and, or;}
    
    public static Builder builder() {
        return new Builder();
    }  
    
    
    public static class Builder {
        private Selection selection = new Selection();
        public Builder values(String... values) {
            selection.values = Arrays.asList(values);
            return this;
        }
        public Builder excludes(String... excludes) {
            selection.excludes = Arrays.asList(excludes);
            return this;
        }
        public Builder props(Map<String,String> props) {
            selection.props = props;
            return this;
        }
        public Builder operatorOr() {
            selection.op = Operator.or;
            return this;
        }
        public Builder operatorAnd() {
            selection.op = Operator.or;
            return this;
        }
        public Selection build() {
            return selection;
        }
    }
}