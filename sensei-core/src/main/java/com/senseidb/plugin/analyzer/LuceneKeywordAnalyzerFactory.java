package com.senseidb.plugin.analyzer;

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

/**
 * A {@code SenseiPluginFactory} that instantiates a {@link KeywordAnalyzer}. Example configuration:
 *
 * <pre>
 * sensei.index.analyzer.class=com.senseidb.plugin.analyzer.LuceneKeywordAnalyzerFactory
 * </pre>
 *
 *
 * @author jgrande
 *
 */
public class LuceneKeywordAnalyzerFactory implements SenseiPluginFactory<Analyzer> {

    @Override
    public Analyzer getBean(Map<String, String> initProperties, String fullPrefix, SenseiPluginRegistry pluginRegistry) {
        return new KeywordAnalyzer();
    }

}
