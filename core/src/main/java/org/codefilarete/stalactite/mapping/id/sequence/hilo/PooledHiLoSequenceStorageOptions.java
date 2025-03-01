package org.codefilarete.stalactite.mapping.id.sequence.hilo;

/**
 * Options for storing a sequence in the database.
 * 
 * @author Guillame Mary
 */
public class PooledHiLoSequenceStorageOptions {
	
	public static final PooledHiLoSequenceStorageOptions DEFAULT = new PooledHiLoSequenceStorageOptions("sequence_table", "sequence_name", "next_val");
	
	public static final PooledHiLoSequenceStorageOptions HIBERNATE_DEFAULT = new PooledHiLoSequenceStorageOptions("hibernate_sequences", "sequence_name", "next_val");
	
	private final String table;
	private final String sequenceNameColumn;
	private final String valueColumn;
	
	/**
	 * Basic constructor.
	 * 
	 * @param table the table of storage
	 * @param sequenceNameColumn the column name for the sequence name
	 * @param valueColumn the column name for the value
	 */
	public PooledHiLoSequenceStorageOptions(String table, String sequenceNameColumn, String valueColumn) {
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
