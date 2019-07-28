package org.gama.stalactite.persistence.id;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinder;

/**
 * @author Guillaume Mary
 */
public interface Identified<I> {
	
	Identifier<I> getId();
	
	/**
	 * Gives a {@link ParameterBinder} for a general {@link Identified} to be declared in a {@link org.gama.stalactite.sql.binder.ParameterBinderRegistry}
	 * for SQL write operation purpose : it will use the surrogate id as a value for the {@link PreparedStatement}.
	 * The returned {@link ParameterBinder} has no purpose for selection because it doesn't know how to build a fullfilled instance. Even if
	 * it is called, the result is ignored by {@link org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer} which cleanly handle
	 * instanciation and filling of the target.
	 * 
	 * @param parameterBinder the surrogate {@link ParameterBinder} (can be for primitive type because null is already handled by this method result)
	 * @param <I> the type of the surrogate {@link Identifier}
	 * @return a new {@link ParameterBinder} which will wrap/unwrap the result of parameterBinder
	 * @see org.gama.stalactite.sql.binder.DefaultParameterBinders
	 */
	static <I> ParameterBinder<Identified<I>> identifiedBinder(ParameterBinder<I> parameterBinder) {
		return new NullAwareParameterBinder<>(new ParameterBinder<Identified<I>>() {
			@Override
			public Identified<I> get(ResultSet resultSet, String columnName) {
				// we can't instantiate the right Identified because we don't have its class, moreover we should fill the instance properties
				// but we don't have the material to do it, hence the select decoding process is done differenly elsewhere in
				// StrategyJoinsRowTransformer, so we can return anything here 
				return null;
			}
			
			@Override
			public void set(PreparedStatement statement, int valueIndex, Identified<I> value) throws SQLException {
				parameterBinder.set(statement, valueIndex, value.getId().getSurrogate());
			}
		});
	}
}
