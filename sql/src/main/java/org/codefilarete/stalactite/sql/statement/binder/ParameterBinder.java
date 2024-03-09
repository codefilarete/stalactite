package org.codefilarete.stalactite.sql.statement.binder;

import org.codefilarete.tool.function.SerializableThrowingFunction;

/**
 * Merge of {@link ResultSetReader} et {@link PreparedStatementWriter}
 * 
 * @author Guillaume Mary
 */
public interface ParameterBinder<T> extends ResultSetReader<T>, PreparedStatementWriter<T> {
	
	/**
	 * Returns a new {@link ParameterBinder} which adds some converters around read and write steps of this instance.
	 * 
	 * @param readConverter the converter to be applied after current instance database read step
	 * @param writeConverter the converter to be applied before current instance database write step
	 * @return a new {@link ParameterBinder} of type O which converts it to T through given converters
	 * @param <O> type of returned {@link ParameterBinder}
	 */
	default <O> ParameterBinder<O> wrap(SerializableThrowingFunction<T, O, ? extends Throwable> readConverter,
										SerializableThrowingFunction<O, T, ? extends Throwable> writeConverter) {
		return new LambdaParameterBinder<>(this, readConverter, writeConverter);
	}
}
