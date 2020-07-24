package org.gama.stalactite.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Wrapper around another {@link ParameterBinder} to handle null value
 * 
 * @author Guillaume Mary
 */
public class NullAwareParameterBinder<T> implements ParameterBinder<T> {
	
	/**
	 * A particular binder that will always set null values onto SQL {@link java.sql.PreparedStatement}
	 */
	public static final ParameterBinder ALWAYS_SET_NULL_INSTANCE = new ParameterBinder() {
		@Override
		public void set(PreparedStatement preparedStatement, int valueIndex, Object value) throws SQLException {
			preparedStatement.setObject(valueIndex, null);
		}
		
		@Override
		public Object doGet(ResultSet resultSet, String columnName) {
			throw new UnsupportedOperationException("This code should never be called because it's only aimed at writing parameters, not reading");
		}
	};
	
	private final NullAwareResultSetReader<T> nullAwareResultSetReader;
	
	private final NullAwarePreparedStatementWriter<T> nullAwarePreparedStatementWriter;
	
	public NullAwareParameterBinder(ParameterBinder<T> surrogate) {
		this(new NullAwareResultSetReader<>(surrogate), new NullAwarePreparedStatementWriter<>(surrogate));
	}
	
	public NullAwareParameterBinder(NullAwareResultSetReader<T> nullAwareResultSetReader,
									NullAwarePreparedStatementWriter<T> nullAwarePreparedStatementWriter) {
		this.nullAwareResultSetReader = nullAwareResultSetReader;
		this.nullAwarePreparedStatementWriter = nullAwarePreparedStatementWriter;
	}
	
	@Override
	public T doGet(ResultSet resultSet, String columnName) {
		return nullAwareResultSetReader.get(resultSet, columnName);
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, T value) throws SQLException {
		nullAwarePreparedStatementWriter.set(statement, valueIndex, value);
	}
}
