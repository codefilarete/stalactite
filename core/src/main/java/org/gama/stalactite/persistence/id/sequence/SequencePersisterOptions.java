package org.gama.stalactite.persistence.id.sequence;

/**
 * Configuration class of a {@link SequencePersister}
 * 
 * @author Guillame Mary
 */
public class SequencePersisterOptions {
	
	public static final SequencePersisterOptions DEFAULT = new SequencePersisterOptions("sequence_table", "sequence_name", "next_val");
	
	private final String table;
	private final String sequenceNameColumn;
	private final String valueColumn;
	
	public SequencePersisterOptions(String table, String sequenceNameColumn, String valueColumn) {
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
