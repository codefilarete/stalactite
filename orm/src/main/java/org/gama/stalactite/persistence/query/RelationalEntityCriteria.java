package org.gama.stalactite.persistence.query;

import java.util.Collection;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.engine.IPersister.EntityCriteria;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

/**
 * @author Guillaume Mary
 */
public interface RelationalEntityCriteria<C> extends EntityCriteria<C> {
	
	<S extends Collection<A>, A, B> RelationalEntityCriteria<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
}
