package com.senseidb.search.req;


import java.util.Set;


/**
 * Used to customize search request before it's issued from the broker per each node.
 *
 * @author Dmytro Ivchenko
 */
public interface SenseiRequestCustomizer
{
  /**
   * Customizes request by changing some fields.
   * It can change existing request or produce a new instance of {@link SenseiRequest}.
   * @param request request to customize
   * @param partitions partitions against which this request if issued. Typically it's a single partition per each node.
   * @return customized request
   */
  public SenseiRequest customize(SenseiRequest request, Set<Integer> partitions);
}
