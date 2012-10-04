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
package com.senseidb.indexing.hadoop.util;

public interface SenseiJobConfig {
	
	public static final String NUM_SHARDS = "sensei.num.shards";
	public static final String INDEX_PATH = "sensei.index.path";
	public static final String USE_COMPOUND_FILE = "sensei.use.compound.file";
	public static final String INPUT_FORMAT = "sensei.input.format";
	public static final String INDEX_SHARDS = "sensei.index.shards";
	public static final String MAX_FIELD_LENGTH = "sensei.max.field.length";
	public static final String DISTRIBUTION_POLICY = "sensei.distribution.policy";
	public static final String MAPINPUT_CONVERTER = "sensei.mapinput.converter";
	public static final String DOCUMENT_ANALYZER_VERSION = "sensei.document.analyzer.version";
	public static final String DOCUMENT_ANALYZER = "sensei.document.analyzer";
	public static final String MAX_RAMSIZE_BYTES = "sensei.max.ramsize.bytes";
	public static final String MAX_NUM_SEGMENTS = "sensei.max.num.segments";
	public static final String SCHEMA_FILE_URL = "sensei.schema.file.url";
	public static final String FORCE_OUTPUT_OVERWRITE = "sensei.force.output.overwrite";
	public static final String INPUT_DIRS = "sensei.input.dirs";
	public static final String OUTPUT_DIR = "sensei.output.dir";
	public static final String INDEX_SUBDIR_PREFIX = "sensei.index.prefix";
	
}
