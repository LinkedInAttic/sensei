package com.senseidb.search.client.json.req;

/**
 * Represents result pagination.
 * Contains :
    - the number of search results to return
      the starting offset of search results
 */
public class Paging {
    /**
     * the number of search results to return

     */
    private int size;
    /**
     * the starting offset of search results

     */
    private int offset;
    public Paging() {
        // TODO Auto-generated constructor stub
    }
    public Paging(int size, int offset) {
        super();
        this.size = size;
        this.offset = offset;
    }

}
