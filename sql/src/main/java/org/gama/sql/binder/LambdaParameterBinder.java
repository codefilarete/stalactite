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
	
	
	@FunctionalInterface
	interface ResultSetReader<O> {
		O get(ResultSet resultSet, String columnName) throws SQLException;
	}
	
	@FunctionalInterface
	interface PreparedStatementWriter<I> {
		void set(PreparedStatement preparedStatement, Integer index, I value) throws SQLException;
	}
	
	private final ResultSetReader<T> resultSetReader;
	private final PreparedStatementWriter<T> preparedStatementWriter;
	
	public LambdaParameterBinder(ResultSetReader<T> resultSetReader, PreparedStatementWriter<T> preparedStatementWriter) {
		this.resultSetReader = resultSetReader;
		this.preparedStatementWriter = preparedStatementWriter;
	}
	
	@Override
	public T get(String columnName, ResultSet resultSet) throws SQLException {
		return resultSetReader.get(resultSet, columnName);
	}
	
	@Override
	public void set(int valueIndex, T value, PreparedStatement statement) throws SQLException {
		preparedStatementWriter.set(statement, valueIndex, value);
	}
}
