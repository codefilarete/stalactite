package org.gama.stalactite.persistence.id;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Predicate;

import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.binder.ParameterBinder;

/**
 * @author Guillaume Mary
 */
public interface Identified<I> {
	
	Identifier<I> getId();
	
	/** Sortcut to check if a {@link Identified} is persisted or not */
	Predicate<Identified> PERSISTED_PREDICATE = target -> target.getId().isPersisted();
	
	/** Sortcut to check if a {@link Identified} is not persisted */
	Predicate<Identified> NON_PERSISTED_PREDICATE = PERSISTED_PREDICATE.negate();
	
	/**
	 * Gives a {@link ParameterBinder} for a general {@link Identified} to be declared in a {@link org.gama.sql.binder.ParameterBinderRegistry}
	 * for SQL write operation purpose : it will use the surrogate id as a value for the {@link PreparedStatement}.
	 * The returned {@link ParameterBinder} has no purpose for selection because it doesn't know how to build a fullfilled instance. Even if
	 * it is called, the result is ignored by {@link org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer} which cleanly handle
	 * instanciation and filling of the target.
	 * 
	 * @param lambdaParameterBinder the surrogate {@link ParameterBinder} (can be for primitive type because null is already handled by this method result)
	 * @param <I> the type of the surrogate {@link Identifier}
	 * @return a new {@link ParameterBinder} which will wrap/unwrap the result of lambdaParameterBinder
	 * @see org.gama.sql.binder.DefaultParameterBinders
	 */
	static <I> ParameterBinder<Identified<I>> identifiedBinder(LambdaParameterBinder<I> lambdaParameterBinder) {
		return new NullAwareParameterBinder<>(new ParameterBinder<Identified<I>>() {
			@Override
			public Identified<I> get(ResultSet resultSet, String columnName) {
				// we can't instantiate the right Identified because we don't have its class, moreover we should fill the instance property
				// but we don't have the material to do it, as such, the select decoding process is done differenly elsewhere in
				// StrategyJoinsRowTransformer, so we can return anything here 
				return null;
			}
			
			@Override
			public void set(PreparedStatement statement, int valueIndex, Identified<I> value) throws SQLException {
				lambdaParameterBinder.set(statement, valueIndex, value.getId().getSurrogate());
			}
		});
	}
}
