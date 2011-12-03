package com.sensei.search.client.json.req;

import java.util.List;

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