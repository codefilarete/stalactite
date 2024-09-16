package org.codefilarete.stalactite.query;

import java.util.Collection;

import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface RelationalEntityCriteria<C, SELF extends RelationalEntityCriteria<C, SELF>> extends EntityCriteria<C, SELF> {
	
	<S extends Collection<A>, A, B> SELF andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B, ?> operator);
}
