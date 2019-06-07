package org.gama.stalactite.persistence.query;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.query.model.AbstractOperator;
import org.gama.stalactite.query.model.operand.Greater;

/**
 * API to follow for writing criteria on some entity
 * 
 * @param <C> root entity type
 * @author Guillaume Mary
 */
public interface EntityCriteria<C> extends Iterable<ValueAccessPointCriterion> {
	
	<O> EntityCriteria<C> and(SerializableFunction<C, O> getter, AbstractOperator<O> operand);
	
	<O> EntityCriteria<C> and(SerializableBiConsumer<C, O> setter, AbstractOperator<O> operand);
	
	<O> EntityCriteria<C> or(SerializableFunction<C, O> getter, AbstractOperator<O> operand);
	
	<O> EntityCriteria<C> or(SerializableBiConsumer<C, O> setter, AbstractOperator<O> operand);
	
	<A, B> EntityCriteria<C> and(SerializableBiConsumer<C, A> setter, SerializableFunction<A, B> getter, AbstractOperator<B> operand);
}
