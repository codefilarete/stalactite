package org.codefilarete.stalactite.persistence.id.sequence;

/**
 * Options for storing a sequence in the database.
 * 
 * @author Guillame Mary
 */
public class SequenceStorageOptions {
	
	public static final SequenceStorageOptions DEFAULT = new SequenceStorageOptions("sequence_table", "sequence_name", "next_val");
	
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
	public SequenceStorageOptions(String table, String sequenceNameColumn, String valueColumn) {
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
