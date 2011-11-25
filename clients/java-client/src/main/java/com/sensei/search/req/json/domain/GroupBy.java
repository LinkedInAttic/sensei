package com.sensei.search.req.json.domain;

import java.util.List;

public class GroupBy {
    List<String> columns;
    public GroupBy(List<String> columns) {
        this.columns = columns;
    }
}