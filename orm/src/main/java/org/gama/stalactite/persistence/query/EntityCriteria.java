package org.gama.stalactite.persistence.query;

import java.util.Collection;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

/**
 * API to follow for writing criteria on some entity
 * 
 * @param <C> root entity type
 * @author Guillaume Mary
 */
public interface EntityCriteria<C> {
	
	<O> EntityCriteria<C> and(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
	
	<O> EntityCriteria<C> and(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
	
	<O> EntityCriteria<C> or(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
	
	<O> EntityCriteria<C> or(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
	
	<A, B> EntityCriteria<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
	
	<S extends Collection<A>, A, B> EntityCriteria<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
}
