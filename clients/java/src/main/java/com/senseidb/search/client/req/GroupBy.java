package com.senseidb.search.client.req;

import java.util.List;

/**
 * Group-by field: the field name used for the group-by operation (also called field collapsing )
 *
 */
public class GroupBy {
    private List<String> columns;
    private int top;
    public GroupBy() {
	}
    public GroupBy(List<String> columns, int top) {
        this.columns = columns;
        this.top = top;
    }
}