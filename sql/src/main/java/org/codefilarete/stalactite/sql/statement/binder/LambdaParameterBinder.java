package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.codefilarete.tool.function.SerializableThrowingFunction;

/**
 * A {@link ParameterBinder} aimed at simplifying {@link ResultSet} and {@link PreparedStatement} access thanks to method references.
 * 
 * @author Guillaume Mary
 */
public class LambdaParameterBinder<T> implements ParameterBinder<T> {
	
	private final ResultSetReader<T> resultSetReader;
	private final PreparedStatementWriter<T> preparedStatementWriter;
	private final ParameterBinder<?> delegate;
	
	/**
	 * Default basic constructor that delegates its mechanism to a given {@link ResultSetReader} and a {@link PreparedStatementWriter}
	 * 
	 * @param resultSetReader a {@link ResultSetReader}
	 * @param preparedStatementWriter a {@link PreparedStatementWriter}
	 */
	public LambdaParameterBinder(ResultSetReader<T> resultSetReader, PreparedStatementWriter<T> preparedStatementWriter) {
		this.resultSetReader = resultSetReader;
		this.preparedStatementWriter = preparedStatementWriter;
		this.delegate = null;
	}
	
	/**
	 * Composes a new {@link LambdaParameterBinder} from another one which is wrapped by 2 {@link Function}s for converting input and output values
	 * 
	 * @param delegate the root {@link LambdaParameterBinder}
	 * @param resultSetConverter the converter applied on the value read by the root binder
	 * @param statementInputConverter the converter applied on the value passed to the root binder
	 * @param <I> type of the root binder's value type, which is the Java type of the read and written value from/to the database
	 */
	public <I> LambdaParameterBinder(ParameterBinder<I> delegate,
									 SerializableThrowingFunction<I, T, ? extends Throwable> resultSetConverter,
									 SerializableThrowingFunction<T, I, ? extends Throwable> statementInputConverter) {
		this.resultSetReader = delegate.thenApply(resultSetConverter);
		this.preparedStatementWriter = delegate.preApply(statementInputConverter);
		this.delegate = delegate;
	}
	
	@Override
	public Class<T> getType() {
		return resultSetReader.getType();
	}
	
	@Override
	public <O> Class<O> getColumnType() {
		return (Class<O>) (this.delegate == null ? getType() : delegate.getColumnType());
	}
	
	@Override
	public T doGet(ResultSet resultSet, String columnName) {
		return resultSetReader.get(resultSet, columnName);
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, T value) throws SQLException {
		preparedStatementWriter.set(statement, valueIndex, value);
	}
}
