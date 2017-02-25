package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.sql.NullAwarePreparedStatementWriter;

/**
 * Wrapper around another {@link ParameterBinder} to handle null value
 * 
 * @author Guillaume Mary
 */
public class NullAwareParameterBinder<T> implements ParameterBinder<T> {
	
	private final NullAwareResultSetReader<T> nullAwareResultSetReader;
	
	private final NullAwarePreparedStatementWriter<T> nullAwarePreparedStatementWriter;
	
	public NullAwareParameterBinder(ParameterBinder<T> surrogate) {
		this.nullAwareResultSetReader = new NullAwareResultSetReader<>(surrogate);
		this.nullAwarePreparedStatementWriter = new NullAwarePreparedStatementWriter<T>(surrogate);
	}
	
	@Override
	public T get(ResultSet resultSet, String columnName) throws SQLException {
		return nullAwareResultSetReader.get(resultSet, columnName);
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, T value) throws SQLException {
		nullAwarePreparedStatementWriter.set(statement, valueIndex, value);
	}
}
