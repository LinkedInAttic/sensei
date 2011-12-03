package com.sensei.search.client.json.req;

public class Sort {
    private String field;
    private String order;
    
    public static Sort asc(String field) {
        Sort sort = new Sort();
        sort.field = field;
        sort.order = Order.asc.name();
        return sort;
    }
    public static Sort desc(String field) {
        Sort sort = new Sort();
        sort.field = field;
        sort.order = Order.desc.name();
        return sort;
    }
    public static Sort byRelevance() {
        Sort sort = new Sort();
        sort.field = "relevance";       
        return sort;
    }
    public static enum Order {
        desc, asc;
    }
    public String getField() {
        return field;
    }
    public Order getOrder() {
        if (order == null) {
            return null;
        }
        return Order.valueOf(order);
    }
    
}
