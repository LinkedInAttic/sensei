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
 *     <li><code>pattern</code>: a regular expression delimiting tokens (default: "<code>\\s+</code>")
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
        Pattern pattern = Pattern.compile(MapUtils.getString(initProperties, "pattern", "\\s+"));
        boolean toLowerCase = MapUtils.getBoolean(initProperties, "toLowerCase", true);
        String stopWordsStr = MapUtils.getString(initProperties, "stopWords", "");
        Set<String> stopWords = new HashSet<String>(Arrays.asList(stopWordsStr.split("\\s*,\\s*")));
        return new PatternAnalyzer(matchVersion, pattern, toLowerCase, stopWords);
    }

}
