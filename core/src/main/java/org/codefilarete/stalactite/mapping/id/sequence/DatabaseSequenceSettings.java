package org.codefilarete.stalactite.mapping.id.sequence;

/**
 * @author Guillaume Mary
 */
public class DatabaseSequenceSettings {
	
	private final Integer initialValue;
	
	private final Integer batchSize;
	
	private final String schemaName;
	
	public DatabaseSequenceSettings(Integer initialValue, Integer batchSize) {
		this(initialValue, batchSize, null);
	}
	
	public DatabaseSequenceSettings(Integer initialValue, Integer batchSize, String schemaName) {
		this.initialValue = initialValue;
		this.batchSize = batchSize;
		this.schemaName = schemaName;
	}
	
	public Integer getInitialValue() {
		return initialValue;
	}
	
	public Integer getBatchSize() {
		return batchSize;
	}
	
	public String getSchemaName() {
		return schemaName;
	}
}