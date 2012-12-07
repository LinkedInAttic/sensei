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
