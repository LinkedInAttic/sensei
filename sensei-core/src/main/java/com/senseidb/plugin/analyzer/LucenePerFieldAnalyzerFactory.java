package com.senseidb.plugin.analyzer;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

/**
 * A {@code SenseiPluginFactory} that instantiates a {@link PerFieldAnalyzerWrapper}. It reads
 * the field names as a comma-separated list from the <code>fields</code> property. To specify
 * the analyzer for each field, you must do it the same way you normally would, but appending
 * <code>.field.<i>fieldname</i></code> to the usual prefix. You can set the default analyzer
 * with the <code>default</code> property. If this property is missing, the default analyzer
 * is set to {@link KeywordAnalyzer}. Example configuration:
 *
 * <pre>
 * sensei.index.analyzer.class=com.senseidb.plugin.analyzer.LucenePerFieldAnalyzerFactory
 * sensei.index.analyzer.default.class=com.senseidb.plugin.analyzer.LuceneKeywordAnalyzerFactory
 * sensei.index.analyzer.fields=content_tokenized,content_keyword
 *
 * sensei.index.analyzer.fields.content_tokenized.class=com.senseidb.plugin.analyzer.LucenePatternAnalyzerFactory
 * sensei.index.analyzer.fields.content_tokenized.pattern=[ -_./:]
 *
 * sensei.index.analyzer.fields.content_keyword.class=com.senseidb.plugin.analyzer.LuceneKeywordAnalyzerFactory
 * </pre>
 *
 * @author jgrande
 *
 */
public class LucenePerFieldAnalyzerFactory implements SenseiPluginFactory<Analyzer> {

    @Override
    public Analyzer getBean(Map<String, String> initProperties, String fullPrefix, SenseiPluginRegistry pluginRegistry) {
        Analyzer defaultAnalyzer = pluginRegistry.getBeanByFullPrefix(fullPrefix + ".default", Analyzer.class);
        if (defaultAnalyzer == null) {
            defaultAnalyzer = new KeywordAnalyzer();
        }

        String[] fields = MapUtils.getString(initProperties, "fields", "").split("\\s*,\\s*");
        Map<String, Analyzer> analyzers = new HashMap<String, Analyzer>();
        for (String field: fields) {
            String analyzerPrefix = fullPrefix + ".fields." + field;
            Analyzer analyzer = pluginRegistry.getBeanByFullPrefix(analyzerPrefix, Analyzer.class);
            analyzers.put(field, analyzer);
        }

        return new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzers);
    }

}
