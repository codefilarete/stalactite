package org.gama.stalactite.persistence.id;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * Equivalent of {@link StatefullIdentifier} for a more end-user usage.
 * Its class hierarchy implements the different cases.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 * @see PersistableIdentifier
 * @see PersistedIdentifier
 */
public interface Identifier<T> extends StatefullIdentifier<T> {
	
	/**
	 * Gives a {@link ParameterBinder} for a generic {@link Identifier}
	 * @param lambdaParameterBinder the surrogate {@link ParameterBinder} (can be for primitive type because null is already handled by this method result)
	 * @param <I> the type of the surrogate {@link Identifier}
	 * @return a new {@link ParameterBinder} which will wrap/unwrap the result of lambdaParameterBinder
	 * @see org.gama.sql.binder.DefaultParameterBinders
	 */
	static <I> ParameterBinder<StatefullIdentifier<I>> identifierBinder(LambdaParameterBinder<I> lambdaParameterBinder) {
		return new NullAwareParameterBinder<>(new ParameterBinder<StatefullIdentifier<I>>() {
			@Override
			public StatefullIdentifier<I> get(String columnName, ResultSet resultSet) throws SQLException {
				return new PersistedIdentifier<>(lambdaParameterBinder.get(columnName, resultSet));
			}
			
			@Override
			public void set(int valueIndex, StatefullIdentifier<I> value, PreparedStatement statement) throws SQLException {
				lambdaParameterBinder.set(valueIndex, value.getSurrogate(), statement);
			}
		});
	}
}
