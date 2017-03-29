package org.gama.stalactite.persistence.id;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
	 * @param parameterBinder the surrogate {@link ParameterBinder} (can be for primitive type because null is already handled by this method result)
	 * @param <I> the type of the surrogate {@link Identifier}
	 * @return a new {@link ParameterBinder} which will wrap/unwrap the result of parameterBinder
	 * @see org.gama.sql.binder.DefaultParameterBinders
	 */
	static <I> ParameterBinder<StatefullIdentifier<I>> identifierBinder(ParameterBinder<I> parameterBinder) {
		return new NullAwareParameterBinder<>(new ParameterBinder<StatefullIdentifier<I>>() {
			@Override
			public StatefullIdentifier<I> get(ResultSet resultSet, String columnName) throws SQLException {
				return new PersistedIdentifier<>(parameterBinder.get(resultSet, columnName));
			}
			
			@Override
			public void set(PreparedStatement statement, int valueIndex, StatefullIdentifier<I> value) throws SQLException {
				parameterBinder.set(statement, valueIndex, value.getSurrogate());
			}
		});
	}
}
