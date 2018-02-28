package org.gama.sql.binder;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * A {@link ParameterBinder} aimed at simplifying {@link ResultSet} and {@link PreparedStatement} access thanks to method references.
 * 
 * @author Guillaume Mary
 */
public class LambdaParameterBinder<T> implements ParameterBinder<T> {
	
	private final ResultSetReader<T> resultSetReader;
	private final PreparedStatementWriter<T> preparedStatementWriter;
	
	/**
	 * Default basic constructor that delegates its mecanism to a given {@link ResultSetReader} and a {@link PreparedStatementWriter}
	 * 
	 * @param resultSetReader a {@link ResultSetReader}
	 * @param preparedStatementWriter a {@link PreparedStatementWriter}
	 */
	public LambdaParameterBinder(@Nonnull ResultSetReader<T> resultSetReader, @Nonnull PreparedStatementWriter<T> preparedStatementWriter) {
		this.resultSetReader = resultSetReader;
		this.preparedStatementWriter = preparedStatementWriter;
	}
	
	/**
	 * Composes a new {@link LambdaParameterBinder} from a root one that is wrapped by 2 {@link Function}s for converting input and output values
	 * 
	 * @param surrogate the root {@link LambdaParameterBinder}
	 * @param resultSetConverter the converter applied on the value read by the root binder
	 * @param statementInputConverter the converter applied on the value passed to the root binder
	 * @param <I> type of the root binder's value type
	 */
	public <I> LambdaParameterBinder(LambdaParameterBinder<I> surrogate, Function<I, T> resultSetConverter, Function<T, I> statementInputConverter) {
		this.resultSetReader = surrogate.resultSetReader.thenApply(resultSetConverter);
		this.preparedStatementWriter = surrogate.preparedStatementWriter.preApply(statementInputConverter);
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
