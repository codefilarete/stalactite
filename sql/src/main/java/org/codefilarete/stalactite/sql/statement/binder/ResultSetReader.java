package org.codefilarete.stalactite.sql.statement.binder;

import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.SerializedLambda;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.codefilarete.reflection.MethodReferenceCapturer.MethodDefinition;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.function.SerializableThrowingBiFunction;
import org.codefilarete.tool.function.ThrowingBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.reflection.MethodReferenceCapturer.giveArgumentTypes;
import static org.codefilarete.reflection.MethodReferences.buildSerializedLambda;

/**
 * An interface that allows to use {@link ResultSet#getXXX(...)} method to be used as method reference.
 * See {@link DefaultResultSetReaders} for some available ones.
 * See {@link PreparedStatementWriter} for its equivalence to write to {@link java.sql.PreparedStatement}, or
 * {@link ParameterBinder} to accomplish both.
 * 
 * @author Guillaume Mary
 * @see PreparedStatementWriter
 * @see DefaultResultSetReaders
 */
public interface ResultSetReader<I> {
	
	/**
	 * Creates a {@link ResultSetReader} from a method reference.
	 * Its type is deduced from given method reference by introspection of it.
	 * 
	 * @param resultSetGetter one method of {@link ResultSet}
	 * @return a new {@link ResultSetReader} which calls given {@link ResultSet} method
	 * @param <O> the type retrieved from the {@link ResultSet}
	 */
	static <O> ResultSetReader<O> ofMethodReference(SerializableThrowingBiFunction<ResultSet, String, O, SQLException> resultSetGetter) {
		Class<O> argumentType = giveArgumentTypes(buildSerializedLambda(resultSetGetter)).getReturnType();
		return new LambdaResultSetReader<>(resultSetGetter, argumentType);
	}
	
	/**
	 * Reads column <code>columnName</code> returned by <code>resultSet</code>.
	 * This implementation wraps any exception in a {@link BindingException} and in particular wraps {@link ClassCastException} for better message
	 * handling.
	 * Subclasses are expected to implement {@link #doGet(ResultSet, String)}
	 *
	 * @param resultSet the {@link ResultSet} to read
	 * @param columnName the column to be read from the given {@link ResultSet}
	 * @return content of <code>columnName</code>, typed according to <code>column</code>
	 */
	default I get(ResultSet resultSet, String columnName) {
		try {
			return doGet(resultSet, columnName);
		} catch (SQLException e) {
			throw new BindingException("Error while reading column '" + columnName + "'", e);
		} catch (ClassCastException e) {
			String[] classNames = e.getMessage().split(" cannot be cast to ");
			CharSequence ellipsedValue = Strings.ellipsis(String.valueOf(DefaultResultSetReaders.OBJECT_READER.get(resultSet, columnName)), 15);
			throw new BindingException("Error while reading column '" + columnName + "' : trying to read '" + ellipsedValue + "' as " + classNames[1]
					+ " but was " + classNames[0], e);
		}
	}
	
	Class<I> getType();
	
	/**
	 * Method expected to be overridden for really reading {@link ResultSet} value if one want to benefit from exception handling by {@link #get(ResultSet, String)}
	 *
	 * @param resultSet the {@link ResultSet} to read
	 * @param columnName the column to be read from the given {@link ResultSet}
	 * @return content of <code>columnName</code>, typed according to <code>column</code>
	 * @throws SQLException the exception thrown be the underlying access to the {@link ResultSet}
	 */
	I doGet(ResultSet resultSet, String columnName) throws SQLException;
	
	/**
	 * Builds a new {@link ResultSetReader} from this one by applying a converter on the output object
	 * 
	 * @param converter the {@link Function} that turns output value to the final type
	 * @param <O> final type
	 * @return a new {@link ResultSetReader} based on this one plus a converting {@link Function}
	 * @see PreparedStatementWriter#preApply(SerializableFunction)
	 */
	default <O> ResultSetReader<O> thenApply(SerializableFunction<I, O> converter) {
		// Determining the type of next ResultSetReader: this is based 
		SerializedLambda methodReference = buildSerializedLambda(converter);
		MethodDefinition methodDefinition = giveArgumentTypes(methodReference);
		Class<O> argumentType;
		if (methodReference.getImplMethodKind() == MethodHandleInfo.REF_invokeStatic) {
			argumentType = methodDefinition.getArgumentTypes()[0];
		} else {
			argumentType = methodDefinition.getDeclaringClass();
		}
		return new LambdaResultSetReader<O>((rs, columnName) -> converter.apply(this.get(rs, columnName)), argumentType);
	}
	
	/**
	 * Class that helps to wrap a {@link ResultSet} method as a {@link ResultSetReader} 
	 * @param <I> type read by the reader
	 * @author Guillaume Mary
	 */
	class LambdaResultSetReader<I> implements ResultSetReader<I> {
		
		private final ThrowingBiFunction<ResultSet, String, I, SQLException> delegate;
		
		private final Class<I> type;
		
		public LambdaResultSetReader(ThrowingBiFunction<ResultSet, String, I, SQLException> delegate, Class<I> type) {
			this.delegate = delegate;
			this.type = type;
		}
		
		@Override
		public Class<I> getType() {
			return type;
		}
		
		@Override
		public I doGet(ResultSet resultSet, String columnName) throws SQLException {
			return delegate.apply(resultSet, columnName);
		}
	}
}
