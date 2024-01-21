package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

import org.codefilarete.tool.function.SerializableThrowingTriConsumer;
import org.codefilarete.tool.function.ThrowingTriConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.reflection.MethodReferenceCapturer.giveArgumentTypes;
import static org.codefilarete.reflection.MethodReferences.buildSerializedLambda;

/**
 * An interface that allows to use {@link PreparedStatement#setXXX(...)} methods to be used as method reference.
 * See {@link DefaultPreparedStatementWriters} for some available ones.
 * See {@link ResultSetReader} for its equivalence for reading {@link java.sql.ResultSet}, or
 * {@link ParameterBinder} to accomplish both.
 * 
 * @author Guillaume Mary
 * @see ResultSetReader
 * @see DefaultPreparedStatementWriters
 */
public interface PreparedStatementWriter<I> {
	
	/**
	 * Creates a {@link PreparedStatementWriter} from a method reference.
	 * Its type is deduced from given method reference by introspection of it.
	 *
	 * @param preparedStatementSetter one method of {@link PreparedStatement}
	 * @return a new {@link PreparedStatementWriter} which calls given {@link PreparedStatement} method
	 * @param <O> the type set to the {@link PreparedStatement}
	 */
	static <O> PreparedStatementWriter<O> ofMethodReference(SerializableThrowingTriConsumer<PreparedStatement, Integer, O, SQLException> preparedStatementSetter) {
		Class<O> argumentType = giveArgumentTypes(buildSerializedLambda(preparedStatementSetter)).getReturnType();
		return new LambdaPreparedStatementWriter<>(preparedStatementSetter, argumentType);
	}
	
	/**
	 * Applies <code>value</code> at position <code>valueIndex</code> on <code>statement</code>.
	 *
	 * @param preparedStatement PreparedStatement to be used
	 * @param valueIndex parameter index to be set, value for first parameter of methods <code>Statement.setXXX(..)</code>
	 * @param value value to be passed as second argument of methods <code>Statement.setXXX(..)</code>
	 * @throws SQLException the exception thrown be the underlying access to the {@link PreparedStatement}
	 */
	void set(PreparedStatement preparedStatement, int valueIndex, I value) throws SQLException;
	
	/**
	 * Builds a new {@link PreparedStatementWriter} from this one by applying a converter on the input object
	 * 
	 * @param converter the {@link Function} that turns input value to the colum type
	 * @param <O> type of input accepted by resulting {@link PreparedStatementWriter} 
	 * @return a new {@link PreparedStatementWriter} based on this one plus a converting {@link Function}
	 * @see ResultSetReader#thenApply(SerializableFunction)
	 */
	default <O> PreparedStatementWriter<O> preApply(SerializableFunction<O, I> converter) {
		Class<O> argumentType = giveArgumentTypes(buildSerializedLambda(converter)).getReturnType();
		return new LambdaPreparedStatementWriter<O>((ps, valueIndex, value) -> this.set(ps, valueIndex, converter.apply(value)), argumentType);
	}
	
	Class<I> getType();
	
	/**
	 * Class that helps to wrap a {@link PreparedStatement} method as a {@link PreparedStatementWriter}
	 * @param <I> type written by the writer
	 * @author Guillaume Mary
	 */
	class LambdaPreparedStatementWriter<I> implements PreparedStatementWriter<I> {
		
		private final ThrowingTriConsumer<PreparedStatement, Integer, I, SQLException> delegate;
		
		private final Class<I> type;
		
		public LambdaPreparedStatementWriter(ThrowingTriConsumer<PreparedStatement, Integer, I, SQLException> delegate, Class<I> type) {
			this.delegate = delegate;
			this.type = type;
		}
		
		
		@Override
		public Class<I> getType() {
			return type;
		}
		
		@Override
		public void set(PreparedStatement preparedStatement, int valueIndex, I value) throws SQLException {
			delegate.accept(preparedStatement, valueIndex, value);
		}
	}
}