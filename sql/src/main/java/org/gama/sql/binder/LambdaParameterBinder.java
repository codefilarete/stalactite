package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A {@link ParameterBinder} aimed at simplifying {@link ResultSet} and {@link PreparedStatement} access thanks to method references.
 * 
 * @author Guillaume Mary
 */
public class LambdaParameterBinder<T> implements ParameterBinder<T> {
	
	private final ResultSetReader<T> resultSetReader;
	private final PreparedStatementWriter<T> preparedStatementWriter;
	
	public LambdaParameterBinder(ResultSetReader<T> resultSetReader, PreparedStatementWriter<T> preparedStatementWriter) {
		this.resultSetReader = resultSetReader;
		this.preparedStatementWriter = preparedStatementWriter;
	}
	
	@Override
	public T get(ResultSet resultSet, String columnName) throws SQLException {
		return resultSetReader.get(resultSet, columnName);
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, T value) throws SQLException {
		preparedStatementWriter.set(statement, valueIndex, value);
	}
}
