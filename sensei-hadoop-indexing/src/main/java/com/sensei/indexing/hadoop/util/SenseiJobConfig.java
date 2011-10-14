package com.sensei.indexing.hadoop.util;

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
	
}