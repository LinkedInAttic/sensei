package com.senseidb.search.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.AbstractIndexReaderDecorator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;

public class SenseiIndexReaderDecorator extends AbstractIndexReaderDecorator<BoboIndexReader> {
    private final List<FacetHandler<?>> _facetHandlers;
    private static final Logger logger = Logger.getLogger(SenseiIndexReaderDecorator.class);
    private final List<RuntimeFacetHandlerFactory<?, ?>> _facetHandlerFactories;
    private List<BoboListener> boboListeners = new ArrayList<SenseiIndexReaderDecorator.BoboListener>();

    public SenseiIndexReaderDecorator(List<FacetHandler<?>> facetHandlers, List<RuntimeFacetHandlerFactory<?, ?>> facetHandlerFactories) {
        _facetHandlers = facetHandlers;
        _facetHandlerFactories = facetHandlerFactories;
    }

    public SenseiIndexReaderDecorator() {
        this(null, null);
    }

    public List<FacetHandler<?>> getFacetHandlerList() {
        return _facetHandlers;
    }

    public List<RuntimeFacetHandlerFactory<?, ?>> getFacetHandlerFactories() {
        return _facetHandlerFactories;
    }

    public BoboIndexReader decorate(ZoieIndexReader<BoboIndexReader> zoieReader) throws IOException {
        BoboIndexReader boboReader = null;
        if (zoieReader != null) {
            boboReader = BoboIndexReader.getInstanceAsSubReader(zoieReader, _facetHandlers, _facetHandlerFactories);
        }
        applyListeners(boboReader);
        return boboReader;
    }

    private BoboIndexReader applyListeners(final BoboIndexReader boboReader) {
        for (BoboListener boboListener : boboListeners) {
            boboListener.indexCreated(boboReader);
        }
        boboReader.addReaderFinishedListener(new SenseiIndexReaderFinishedListener(boboListeners));
        return boboReader;

    }

    @Override
    public BoboIndexReader redecorate(BoboIndexReader reader, ZoieIndexReader<BoboIndexReader> newReader, boolean withDeletes)
            throws IOException {
        return applyListeners(reader.copy(newReader));
    }

    public static interface BoboListener {
        public void indexCreated(BoboIndexReader boboIndexReader);

        public void indexDeleted(IndexReader indexReader);
    }

    public void addBoboListener(BoboListener boboListener) {
        boboListeners.add(boboListener);
    }

}

class SenseiIndexReaderFinishedListener implements IndexReader.ReaderFinishedListener {
    private static final Logger log = Logger.getLogger(SenseiIndexReaderFinishedListener.class);
    private final Collection<SenseiIndexReaderDecorator.BoboListener> boboListeners;

    SenseiIndexReaderFinishedListener(Collection<SenseiIndexReaderDecorator.BoboListener> boboListeners) {
        this.boboListeners = boboListeners;
    }

    @Override
    public void finished(IndexReader indexReader) {
        for (SenseiIndexReaderDecorator.BoboListener boboListener : boboListeners) {
            boboListener.indexDeleted(indexReader);
        }
    }
}
