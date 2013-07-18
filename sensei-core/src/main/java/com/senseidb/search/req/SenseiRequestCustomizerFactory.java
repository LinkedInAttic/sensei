package com.senseidb.search.req;

/**
 * Class to instantiate {@link SenseiRequestCustomizer}.
 * It will be called on each broker request.
 *
 * @author Dmytro Ivchenko
 */
public interface SenseiRequestCustomizerFactory {
    /**
     * Creates a new {@link SenseiRequestCustomizer}.
     * @param request request
     * @return instance of {@link SenseiRequestCustomizer}.
     */
    public SenseiRequestCustomizer getRequestCustomizer(SenseiRequest request);
}
