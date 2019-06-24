package org.gama.sql.dml;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.gama.lang.collection.Iterables;
import org.gama.sql.binder.DefaultResultSetReaders;
import org.gama.sql.binder.ResultSetReader;
import org.gama.sql.result.ResultSetIterator;
import org.gama.sql.result.Row;

/**
 * Default implementation of a {@link ResultSet} returned by {@link Statement#getGeneratedKeys()}.
 * It only reads one generated value that must be in the first column of the returned {@link ResultSet}.
 * It assumes that the key is a primitive {@link Long}.
 * As specified by {@link Statement#getGeneratedKeys()}, it expected that a key is returned for each inserted row.
 * 
 * The key is noly used to fill the rows returned by {@link #read(WriteOperation)}, it's not used for ResultSet read.
 * 
 * As databases support differently generated keys feature, you may override some behaviors. For instance if the
 * key is not in the first column or you don't want to read it as a long, you may override {@link #readKey(ResultSet)}.
 * 
 * If only one row is returned even in multi-row statement (Derby behaves like this, see https://issues.apache.org/jira/browse/DERBY-3609),
 * then you may override {@link #read(WriteOperation)} to simulate multiple rows reading.
 * 
 * @author Guillaume Mary
 */
public class GeneratedKeysReader {
	
	private final String keyName;
	
	/** Underlying reader of generated value */
	private final ResultSetReader typeReader;
	
	/**
	 * Constructor dedicated to {@link Long} value reader.
	 * 
	 * @param keyName column name to be read on generated {@link ResultSet}
	 */
	public GeneratedKeysReader(String keyName) {
		this.keyName = keyName;
		this.typeReader = DefaultResultSetReaders.LONG_PRIMITIVE_READER;
	}
	
	/**
	 * Constructor.
	 * 
	 * @param keyName column name to be read on generated {@link ResultSet}
	 * @param typeReader reader of generated key column
	 */
	public GeneratedKeysReader(String keyName, ResultSetReader typeReader) {
		this.keyName = keyName;
		this.typeReader = typeReader;
	}
	
	public String getKeyName() {
		return keyName;
	}
	
	public List<Row> read(WriteOperation writeOperation) throws SQLException {
		try (ResultSet generatedKeys = writeOperation.preparedStatement.getGeneratedKeys()) {
			ResultSetIterator<Row> iterator = new ResultSetIterator<Row>(generatedKeys) {
				@Override
				public Row convert(ResultSet rs) throws SQLException {
					Row toReturn = new Row();
					fillRow(toReturn, rs);
					return toReturn;
				}
			};
			return Iterables.copy(iterator);
		}
	}
	
	protected void fillRow(Row row, ResultSet rs) throws SQLException {
		row.put(keyName, readKey(rs));
	}
	
	protected Object readKey(ResultSet rs) throws SQLException {
		return typeReader.get(rs, getKeyName());
	}
}
