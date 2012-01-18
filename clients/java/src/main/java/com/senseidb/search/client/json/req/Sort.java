package com.senseidb.search.client.json.req;

/**
 * This parameter specifies how the search results should be sorted. The results can be sorted based on one or multiple
    fields, in either ascending or descending order. The value of this parameter consists of a list of comma separated
    strings, each of which can be one of the following values:
    <br>• relevance: this means that the results should be sorted by scores in descending order.
    <br>• relrev: this means that the results should be sorted by scores in ascending order.
    <br>• doc: this means that the results should be sorted by doc ids in ascending order.
    <br>• docrev: this means that the results should be sorted by doc ids in descending order.
    <br>• <field-name>:<direction>: this means that the results should be sorted by field <field-name> in the
    direction of <direction>, which can be either asc or desc.
    Example : Sort Fields Parameters
    <br>sort=relevance
    <br>sort=docrev
    <br>sort=price:desc,color=asc

 *
 */
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
