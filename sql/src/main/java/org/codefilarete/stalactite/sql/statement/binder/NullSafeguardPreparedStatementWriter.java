package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.tool.function.SerializableThrowingTriConsumer;

/**
 * Wrapper around another {@link PreparedStatementWriter} of primitive types to throw an exception on null value with a clearer message than
 * a cryptic {@link NullPointerException}
 * 
 * @author Guillaume Mary
 * @param <C> a primitive type
 */
public class NullSafeguardPreparedStatementWriter<C> implements PreparedStatementWriter<C> {
	
	private final PreparedStatementWriter<C> delegate;
	
	public NullSafeguardPreparedStatementWriter(SerializableThrowingTriConsumer<PreparedStatement, Integer, C, SQLException> preparedStatementSetter) {
		this(PreparedStatementWriter.ofMethodReference(preparedStatementSetter));
	}
	
	public NullSafeguardPreparedStatementWriter(PreparedStatementWriter<C> delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public Class<C> getType() {
		return delegate.getType();
	}
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, C value) throws SQLException {
		if (value == null) {
			// NB : we can't give detail about primitive type because we don't know it
			throw new IllegalArgumentException("Trying to pass null as primitive value");
		}
		delegate.set(preparedStatement, valueIndex, value);
	}
}
