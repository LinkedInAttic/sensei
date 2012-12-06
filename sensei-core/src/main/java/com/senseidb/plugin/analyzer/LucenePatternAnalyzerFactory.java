package com.senseidb.plugin.analyzer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.collections.MapUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PatternAnalyzer;
import org.apache.lucene.util.Version;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

/**
 * A {@code SenseiPluginFactory} that instantiates a {@link PatternAnalyzer}. It can be configured through
 * the following properties:
 *
 * <ul>
 *     <li><code>matchVersion</code>: the Lucene version compatibility (default: LUCENE_35). See {@link Version}.
 *     <li><code>pattern</code>: a regular expression delimiting tokens (default: "<code> +</code>")
 *     <li><code>toLowerCase</code>: if true, tokens are converted to lower case (default: true)
 *     <li><code>stopWords</code>: a comma-separated list of stop words (defaults to an empty list)
 * </ul>
 *
 * Example configuration:
 *
 * <pre>
 * sensei.index.analyzer.class=com.senseidb.plugin.analyzer.LucenePatternAnalyzerFactory
 * sensei.index.analyzer.pattern=[ -_./:]
 * </pre>
 *
 * @author jgrande
 *
 */
public class LucenePatternAnalyzerFactory implements SenseiPluginFactory<Analyzer> {

    @Override
    public Analyzer getBean(Map<String, String> initProperties, String fullPrefix, SenseiPluginRegistry pluginRegistry) {
        Version matchVersion = Version.valueOf(MapUtils.getString(initProperties, "matchVersion", "LUCENE_35"));
        Pattern pattern = Pattern.compile(MapUtils.getString(initProperties, "pattern", " +"));
        boolean toLowerCase = MapUtils.getBoolean(initProperties, "toLowerCase", true);
        String stopWordsStr = MapUtils.getString(initProperties, "stopWords", "");
        Set<String> stopWords = new HashSet<String>(Arrays.asList(stopWordsStr.split(" *, *")));
        return new PatternAnalyzer(matchVersion, pattern, toLowerCase, stopWords);
    }

}
