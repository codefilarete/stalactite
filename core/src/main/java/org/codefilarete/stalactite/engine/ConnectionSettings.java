package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public class ConnectionSettings {
	
	private final int batchSize;
	
	/**
	 * Maximum number of values for a "in" operator.
	 * Must be used in prior to {@link DatabaseVendorSettings#getInOperatorMaxSize()}
	 */
	private final Integer inOperatorMaxSize;
	
	public ConnectionSettings(int batchSize, int inOperatorMaxSize) {
		this.batchSize = batchSize;
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	public Integer getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
}
