package org.gama.sql.dml;

import org.gama.lang.exception.Exceptions;
import org.gama.sql.result.ResultSetIterator;
import org.gama.sql.result.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
	
	public GeneratedKeysReader(String keyName) {
		this.keyName = keyName;
	}
	
	public String getKeyName() {
		return keyName;
	}
	
	public List<Row> read(WriteOperation writeOperation) throws SQLException {
		try (ResultSet generatedKeys = writeOperation.preparedStatement.getGeneratedKeys()) {
			List<Row> toReturn = new ArrayList<>();
			ResultSetIterator<Row> iterator = new ResultSetIterator<Row>(generatedKeys) {
				@Override
				public Row convert(ResultSet rs) {
					Row toReturn = new Row();
					try {
						fillRow(toReturn, rs);
					} catch (SQLException e) {
						throw Exceptions.asRuntimeException(e);
					}
					return toReturn;
				}
			};
			while (iterator.hasNext()) {
				Row row = iterator.next();
				toReturn.add(row);
			}
			return toReturn;
		}
	}
	
	protected void fillRow(Row row, ResultSet rs) throws SQLException {
		row.put(keyName, readKey(rs));
	}
	
	protected Object readKey(ResultSet rs) throws SQLException {
		return rs.getLong(getKeyName());
	}
}
