package org.codefilarete.stalactite.sql.statement.binder;

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
		
		@Override
		public Class getType() {
			return Object.class;
		}
	};
	
	private final NullAwareResultSetReader<T> nullAwareResultSetReader;
	
	private final NullAwarePreparedStatementWriter<T> nullAwarePreparedStatementWriter;
	
	public NullAwareParameterBinder(ParameterBinder<T> delegate) {
		this(new NullAwareResultSetReader<> (delegate), new NullAwarePreparedStatementWriter<> (delegate));
	}
	
	public NullAwareParameterBinder(ResultSetReader<T> resultSetReader,
									PreparedStatementWriter<T> preparedStatementWriter) {
		this(new NullAwareResultSetReader<>(resultSetReader), new NullAwarePreparedStatementWriter<>(preparedStatementWriter));
	}
	
	public NullAwareParameterBinder(NullAwareResultSetReader<T> nullAwareResultSetReader,
									NullAwarePreparedStatementWriter<T> nullAwarePreparedStatementWriter) {
		this.nullAwareResultSetReader = nullAwareResultSetReader;
		this.nullAwarePreparedStatementWriter = nullAwarePreparedStatementWriter;
	}
	
	@Override
	public Class<T> getType() {
		return nullAwareResultSetReader.getType();
	}
	
	@Override
	public <O> Class<O> getColumnType() {
		return this.nullAwareResultSetReader.getColumnType();
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
