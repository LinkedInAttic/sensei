/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.
 */
package com.senseidb.plugin.analyzer;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

/**
 * A {@code SenseiPluginFactory} that instantiates a {@link PerFieldAnalyzerWrapper}. It reads
 * the field names as a comma-separated list from the <code>fields</code> property. To specify
 * the analyzer for each field, you must do it the same way you normally would, but appending
 * <code>.field.<i>fieldname</i></code> to the usual prefix. You can set the default analyzer
 * with the <code>default</code> property. If this property is missing, the default analyzer
 * is set to {@link StandardAnalyzer} with version {@code LUCENE_35}. Example configuration:
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
            defaultAnalyzer = new StandardAnalyzer(Version.LUCENE_35);
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
