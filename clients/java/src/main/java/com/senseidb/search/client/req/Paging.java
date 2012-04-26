package com.senseidb.search.client.req;

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
    private int count;
    /**
     * the starting offset of search results

     */
    private int offset;
    public Paging() {
        // TODO Auto-generated constructor stub
    }
    public Paging(int count, int offset) {
        super();
        this.count = count;
        this.offset = offset;
    }

}
