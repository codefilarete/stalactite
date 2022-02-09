package org.codefilarete.stalactite.persistence.query;

import java.util.Collection;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.stalactite.persistence.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.query.model.AbstractRelationalOperator;

/**
 * @author Guillaume Mary
 */
public interface RelationalEntityCriteria<C> extends EntityCriteria<C> {
	
	<S extends Collection<A>, A, B> RelationalEntityCriteria<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
}
