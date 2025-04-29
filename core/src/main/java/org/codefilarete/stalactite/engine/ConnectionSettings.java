package org.codefilarete.stalactite.engine;

/**
 * Settings for database connections, including batch size for write operations and fetch size for read operations.
 * - Batch size controls how many statements are sent to the database in one batch during write operations.
 * - Fetch size controls how many rows are retrieved at once from the database during read operations.
 * Both act as a performance optimization parameter when writing and retrieving data respectively.
 * 
 * @author Guillaume Mary
 */
public class ConnectionSettings {
	
	public static final int DEFAULT_BATCH_SIZE = 20;

	/**
	 * Batch size for write statements requested to the database. Default value is {@value #DEFAULT_BATCH_SIZE}
	 */
	private final int batchSize;
	
	/**
	 * Fetch size for select statements requested to the database. If null, then default value from JDBC
	 * {@link java.sql.Connection} will be used.
	 */
	private final Integer fetchSize;

	public ConnectionSettings() {
		this(DEFAULT_BATCH_SIZE);
	}
	
	public ConnectionSettings(int batchSize) {
		this(batchSize, null);
	}
	
	public ConnectionSettings(int batchSize, Integer fetchSize) {
		this.batchSize = batchSize;
		this.fetchSize = fetchSize;
	}
	
	/**
	 * Gets the batch size for write operations.
	 * @return the batch size for write operations, {@value #DEFAULT_BATCH_SIZE} by default
	 */
	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * Gets the fetch size for read operations.
	 * @return null if the default JDBC driver value is used
	 */
	public Integer getFetchSize() {
		return fetchSize;
	}
}
