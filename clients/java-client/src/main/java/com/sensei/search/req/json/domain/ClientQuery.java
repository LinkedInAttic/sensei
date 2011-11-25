package com.sensei.search.req.json.domain;

import org.json.JSONObject;

import com.sensei.search.req.json.JsonField;

public class ClientQuery {
    @JsonField("query-string")
    JSONObject jsonQuery;
    public ClientQuery(JSONObject jsonQuery) {
        this.jsonQuery = jsonQuery;           
    }
}