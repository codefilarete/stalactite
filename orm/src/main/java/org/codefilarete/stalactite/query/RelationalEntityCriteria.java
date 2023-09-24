package org.codefilarete.stalactite.query;

import java.util.Collection;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface RelationalEntityCriteria<C> extends EntityCriteria<C> {
	
	@Override
	<O> RelationalEntityCriteria<C> and(SerializableFunction<C, O> getter, ConditionalOperator<O> operator);
	
	@Override
	<O> RelationalEntityCriteria<C> and(SerializableBiConsumer<C, O> setter, ConditionalOperator<O> operator);
	
	@Override
	<O> RelationalEntityCriteria<C> or(SerializableFunction<C, O> getter, ConditionalOperator<O> operator);
	
	@Override
	<O> RelationalEntityCriteria<C> or(SerializableBiConsumer<C, O> setter, ConditionalOperator<O> operator);
	
	@Override
	<A, B> RelationalEntityCriteria<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B> operator);
	
	@Override
	<O> RelationalEntityCriteria<C> and(AccessorChain<C, O> getter, ConditionalOperator<O> operator);
	
	<S extends Collection<A>, A, B> RelationalEntityCriteria<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B> operator);
}
