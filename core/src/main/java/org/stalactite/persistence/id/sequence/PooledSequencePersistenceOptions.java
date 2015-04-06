package org.stalactite.persistence.id.sequence;

/**
 * @author mary
 */
public class PooledSequencePersistenceOptions {
	
	public static final PooledSequencePersistenceOptions DEFAULT = new PooledSequencePersistenceOptions("sequence_table", "sequence_name", "next_val");
	
	private final String table;
	private final String sequenceNameColumn;
	private final String valueColumn;
	
	public PooledSequencePersistenceOptions(String table, String sequenceNameColumn, String valueColumn) {
		this.table = table;
		this.sequenceNameColumn = sequenceNameColumn;
		this.valueColumn = valueColumn;
	}
	
	public String getTable() {
		return table;
	}
	
	public String getSequenceNameColumn() {
		return sequenceNameColumn;
	}
	
	public String getValueColumn() {
		return valueColumn;
	}
}
