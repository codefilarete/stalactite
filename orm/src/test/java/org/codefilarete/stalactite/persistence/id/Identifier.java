package org.codefilarete.stalactite.persistence.id;

import org.codefilarete.stalactite.sql.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.binder.ParameterBinder;

/**
 * Equivalent of {@link StatefulIdentifier} for a more end-user usage.
 * Its class hierarchy implements the different cases.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 * @see PersistableIdentifier
 * @see PersistedIdentifier
 */
public interface Identifier<T> extends StatefulIdentifier<T> {
	
	/** A simple constant to help to reference a {@link Identifier} which surrogate is a {@link Long} */
	Class<Identifier<Long>> LONG_TYPE = (Class<Identifier<Long>>) (Class) Identifier.class;
	
	/**
	 * Gives a {@link ParameterBinder} for a generic {@link Identifier}
	 * @param parameterBinder the surrogate {@link ParameterBinder} (can be for primitive type because null is already handled by this method result)
	 * @param <I> the type of the surrogate {@link Identifier}
	 * @return a new {@link ParameterBinder} which will wrap/unwrap the result of parameterBinder
	 * @see org.codefilarete.stalactite.sql.binder.DefaultParameterBinders
	 */
	static <I> ParameterBinder<StatefulIdentifier<I>> identifierBinder(ParameterBinder<I> parameterBinder) {
		return new NullAwareParameterBinder<>(new LambdaParameterBinder<>(parameterBinder, PersistedIdentifier::new, StatefulIdentifier::getSurrogate));
	}
}
