package org.codefilarete.stalactite.id;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

/**
 * @author Guillaume Mary
 */
public interface Identified<I> {
	
	Identifier<I> getId();
	
	/**
	 * Gives a {@link ParameterBinder} for a general {@link Identified} to be declared in a {@link ParameterBinderRegistry}
	 * for SQL write operation purpose : it will use the delegate id as a value for the {@link PreparedStatement}.
	 * The returned {@link ParameterBinder} has no purpose for selection because it doesn't know how to build a fulfilled instance. Even if
	 * it is called, the result is ignored by {@link EntityTreeInflater} which cleanly handle
	 * instantiation and filling of the target.
	 * 
	 * @param parameterBinder the delegate {@link ParameterBinder} (can be for primitive type because null is already handled by this method result)
	 * @param <I> the type of the delegate {@link Identifier}
	 * @return a new {@link ParameterBinder} which will wrap/unwrap the result of parameterBinder
	 * @see DefaultParameterBinders
	 */
	static <I> ParameterBinder<Identified<I>> identifiedBinder(ParameterBinder<I> parameterBinder) {
		return new NullAwareParameterBinder<>(new ParameterBinder<Identified<I>>() {
			@Override
			public Class<Identified<I>> getType() {
				return (Class<Identified<I>>) (Class) Identified.class;
			}
			
			@Override
			public Identified<I> doGet(ResultSet resultSet, String columnName) {
				// we can't instantiate the right Identified because we don't have its class, moreover we should fill the instance properties
				// but we don't have the material to do it, hence the select decoding process is done differently elsewhere in
				// EntityTreeInflater, so we can return anything here 
				return null;
			}
			
			@Override
			public void set(PreparedStatement statement, int valueIndex, Identified<I> value) throws SQLException {
				parameterBinder.set(statement, valueIndex, value.getId().getDelegate());
			}
		});
	}
}
