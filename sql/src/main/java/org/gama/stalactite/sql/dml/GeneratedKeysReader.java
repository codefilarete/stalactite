package org.codefilarete.stalactite.sql.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.sql.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;

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
 * @param <I> generated keys type
 * @author Guillaume Mary
 */
public class GeneratedKeysReader<I> {
	
	private final String keyName;
	
	/** Underlying reader of generated value */
	private final ResultSetReader<I> typeReader;
	
	/**
	 * Constructor.
	 * 
	 * @param keyName column name to be read on generated {@link ResultSet}
	 * @param typeReader reader of generated key column
	 */
	public GeneratedKeysReader(String keyName, ResultSetReader<I> typeReader) {
		this.keyName = keyName;
		this.typeReader = typeReader;
	}
	
	public String getKeyName() {
		return keyName;
	}
	
	/**
	 * Method that must be called after insert or update operation to read generated values through {@link PreparedStatement#getGeneratedKeys()}.
	 * Please note that {@link PreparedStatement} should have been created with {@link java.sql.Connection#prepareStatement(String, int)} and
	 * {@link Statement#RETURN_GENERATED_KEYS} argument.
	 * 
	 * 
	 * @param writeOperation any insert operation (can't be a {@link PreparedStatement} because some implementations needs more information (see Derby override)
	 * @return a {@link List} of database-generated keys during operation execution
	 * @throws SQLException in case of reading problem
	 */
	public List<I> read(WriteOperation writeOperation) throws SQLException {
		try (ResultSet generatedKeys = writeOperation.preparedStatement.getGeneratedKeys()) {
			return read(generatedKeys);
		}
	}
	
	@VisibleForTesting
	List<I> read(ResultSet generatedKeys) {
		ResultSetIterator<I> iterator = new ResultSetIterator<I>(generatedKeys) {
			@Override
			public I convert(ResultSet rs) throws SQLException {
				return readKey(rs);
			}
		};
		return Iterables.copy(iterator);
	}
	
	protected I readKey(ResultSet rs) throws SQLException {
		return typeReader.get(rs, getKeyName());
	}
}
