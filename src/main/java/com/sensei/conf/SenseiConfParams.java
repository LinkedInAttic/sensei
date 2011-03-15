package com.sensei.conf;

public interface SenseiConfParams {
	public static final String NODE_ID = "sensei.node.id";
	public static final String PARTITIONS = "sensei.node.partitions";

	public static final String SERVER_PORT = "sensei.server.port";
	public static final String SERVER_REQ_THREAD_POOL_SIZE = "sensei.server.requestThreadCorePoolSize";
	public static final String SERVER_REQ_THREAD_POOL_MAXSIZE = "sensei.server.requestThreadMaxPoolSize";
	public static final String SERVER_REQ_THREAD_POOL_KEEPALIVE = "sensei.server.requestThreadKeepAliveTimeSecs";
	
	public static final String SENSEI_CLUSTER_NAME = "sensei.cluster.name";
	public static final String SENSEI_CLUSTER_URL = "sensei.cluster.url";
	public static final String SENSEI_CLUSTER_TIMEOUT = "sensei.cluster.timeout";

	public static final String SENSEI_INDEX_DIR = "sensei.index.directory";

	public static final String SENSEI_INDEX_BATCH_SIZE = "sensei.index.batchSize";
	public static final String SENSEI_INDEX_BATCH_DELAY = "sensei.index.batchDelay";
	public static final String SENSEI_INDEX_BATCH_MAXSIZE = "sensei.index.maxBatchSize";
	public static final String SENSEI_INDEX_REALTIME = "sensei.index.realtime";

	public static final String SENSEI_INDEX_ANALYZER = "sensei.index.analyzer";
	public static final String SENSEI_INDEX_SIMILARITY = "sensei.index.similarity";
}
