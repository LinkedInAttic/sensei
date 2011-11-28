package com.sensei.search.client.json.req;

import org.json.JSONObject;

import com.sensei.search.client.json.JsonField;

public class ClientQuery {
    @JsonField("query-string")
    JSONObject jsonQuery;
    public ClientQuery() {
		// TODO Auto-generated constructor stub
	}
    public ClientQuery(JSONObject jsonQuery) {
        this.jsonQuery = jsonQuery;           
    }
}