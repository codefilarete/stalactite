package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

/**
 * @author Guillaume Mary
 */
public class ConnectionSettings {
	
	private final DataSource dataSource;
	
	private final int batchSize;
	
	/**
	 * Maximum number of values for a "in" operator.
	 * Must be used in prior to {@link DatabaseVendorSettings#getInOperatorMaxSize()}
	 */
	private final Integer inOperatorMaxSize;
	
	private final int connectionOpeningRetryMaxCount;
	
	public ConnectionSettings(DataSource dataSource, int batchSize, int inOperatorMaxSize, int connectionOpeningRetryMaxCount) {
		this.dataSource = dataSource;
		this.batchSize = batchSize;
		this.inOperatorMaxSize = inOperatorMaxSize;
		this.connectionOpeningRetryMaxCount = connectionOpeningRetryMaxCount;
	}
	
	public DataSource getDataSource() {
		return dataSource;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	public Integer getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public int getConnectionOpeningRetryMaxCount() {
		return connectionOpeningRetryMaxCount;
	}
}
