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
	
	/**
	 * Combines with "and" given criteria on property  
	 * 
	 * @param getter a method reference to a getter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O> getter return type, also criteria value
	 * @return this
	 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching getter was not found
	 */
	<O> EntityCriteria<C> and(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
	
	/**
	 * Combines with "and" given criteria on property  
	 *
	 * @param setter a method reference to a setter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O> getter return type, also criteria value
	 * @return this
	 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching setter was not found
	 */
	<O> EntityCriteria<C> and(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
	
	/**
	 * Combines with "or" given criteria on property  
	 *
	 * @param getter a method reference to a getter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O> getter return type, also criteria value
	 * @return this
	 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching getter was not found
	 */
	<O> EntityCriteria<C> or(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
	
	/**
	 * Combines with "or" given criteria on property  
	 *
	 * @param setter a method reference to a setter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O> getter return type, also criteria value
	 * @return this
	 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching setter was not found
	 */
	<O> EntityCriteria<C> or(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
	
	/**
	 * Combines with "and" given criteria on an embedded or one-to-one bean property
	 *
	 * @param getter1 a method reference to the embbeded bean
	 * @param getter2 a method reference to the embbeded bean property
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <A> embedded bean type
	 * @param <B> embbeded bean property type, also criteria value
	 * @return this
	 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching getter was not found
	 */
	<A, B> EntityCriteria<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
	
	/**
	 * Combines with "and" given criteria on a {@link Collection} property  
	 *
	 * @param getter1 a method reference to the embbeded bean
	 * @param getter2 a method reference to the embbeded bean property
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <S> collection type
	 * @param <A> bean type
	 * @param <B> bean property type, also criteria value
	 * @return this
	 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching getter was not found
	 */
	<S extends Collection<A>, A, B> EntityCriteria<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
}
