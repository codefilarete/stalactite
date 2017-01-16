package org.gama.stalactite.persistence.id;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.binder.ParameterBinder;

/**
 * @author Guillaume Mary
 */
public interface Identified<I> {
	
	Identifier<I> getId();
	
	void setId(Identifier<I> id);
	
	/**
	 * Gives a {@link ParameterBinder} for a generic {@link Identifier}
	 * @param lambdaParameterBinder the surrogate {@link ParameterBinder} (can be for primitive type because null is already handled by this method result)
	 * @param <I> the type of the surrogate {@link Identifier}
	 * @return a new {@link ParameterBinder} which will wrap/unwrap the result of lambdaParameterBinder
	 * @see org.gama.sql.binder.DefaultParameterBinders
	 */
	static <I> ParameterBinder<Identified<I>> identifiedBinder(LambdaParameterBinder<I> lambdaParameterBinder) {
		return new NullAwareParameterBinder<>(new ParameterBinder<Identified<I>>() {
			@Override
			public Identified<I> get(String columnName, ResultSet resultSet) throws SQLException {
				return null;//new PersistedIdentifier<>(lambdaParameterBinder.get(columnName, resultSet));
			}
			
			@Override
			public void set(int valueIndex, Identified<I> value, PreparedStatement statement) throws SQLException {
				lambdaParameterBinder.set(valueIndex, value.getId().getSurrogate(), statement);
			}
		});
	}
}
