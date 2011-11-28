package com.sensei.search.client.json.req;

import java.util.List;

public class GroupBy {
    List<String> columns;
    public GroupBy() {
	}
    public GroupBy(List<String> columns) {
        this.columns = columns;
    }
}