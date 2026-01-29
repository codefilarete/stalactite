package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.tool.collection.Arrays;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Contract that allows to create some query criteria based on property accessors
 *
 * @param <C> type of object returned by query execution
 */
public interface EntityCriteria<C, SELF extends EntityCriteria<C, SELF>> {
	
	/**
	 * Combines with "and" given criteria on property
	 *
	 * @param getter   a method reference to a getter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O>      getter return type, also criteria value
	 * @return this
	 * @throws IllegalArgumentException if column matching getter was not found
	 */
	<O> SELF and(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator);
	
	/**
	 * Combines with "and" given criteria on property
	 *
	 * @param setter   a method reference to a setter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O>      getter return type, also criteria value
	 * @return this
	 * @throws IllegalArgumentException if column matching setter was not found
	 */
	<O> SELF and(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator);
	
	<O> SELF and(CriteriaPath<C, O> propertyPath, ConditionalOperator<O, ?> operator);
	
	/**
	 * Combines with "or" given criteria on property
	 *
	 * @param getter   a method reference to a getter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O>      getter return type, also criteria value
	 * @return this
	 * @throws IllegalArgumentException if column matching getter was not found
	 */
	<O> SELF or(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator);
	
	/**
	 * Combines with "or" given criteria on property
	 *
	 * @param setter   a method reference to a setter
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <O>      getter return type, also criteria value
	 * @return this
	 * @throws IllegalArgumentException if column matching setter was not found
	 */
	<O> SELF or(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator);
	
	<O> SELF or(CriteriaPath<C, O> propertyPath, ConditionalOperator<O, ?> operator);
	
	/**
	 * Starts a nested condition and returns it. At SQL rendering time, it should be embedded between parenthesis.
	 * Result must be used to fill the nested condition, not current instance.
	 * After filling it, it must be closed by {@link #endNested()}
	 *
	 * @return a nested condition to be filled
	 */
	EntityCriteria<C, SELF> beginNested();
	
	/**
	 * Ends a nested condition started with {@link #beginNested()}. It returns the enclosing instance (the one on which {@link #beginNested()}
	 * was called).
	 *
	 * @return the enclosing condition
	 */
	EntityCriteria<C, SELF> endNested();
	
	/**
	 * Combines with "and" given criteria on an embedded or one-to-one bean property
	 *
	 * @param getter1  a method reference to the embedded bean
	 * @param getter2  a method reference to the embedded bean property
	 * @param operator operator of the criteria (will be the condition on the matching column)
	 * @param <A>      embedded bean type
	 * @param <B>      embedded bean property type, also criteria value
	 * @return this
	 * @throws IllegalArgumentException if column matching getter was not found
	 */
	<A, B> SELF and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, ConditionalOperator<B, ?> operator);
	
	default <O> SELF and(ValueAccessPoint<C> accessor, ConditionalOperator<O, ?> operator) {
		if (accessor instanceof AccessorChain) {
			return and(((AccessorChain<?, ?>) accessor).getAccessors(), operator);
		} else {
			return and(Arrays.asList(accessor), operator);
		}
	}
	
	default <O> SELF or(ValueAccessPoint<C> accessor, ConditionalOperator<O, ?> operator) {
		if (accessor instanceof AccessorChain) {
			return or(((AccessorChain<?, ?>) accessor).getAccessors(), operator);
		} else {
			return or(Arrays.asList(accessor), operator);
		}
	}
	
	<O> SELF and(List<? extends ValueAccessPoint<?>> accessors, ConditionalOperator<O, ?> operator);
	
	<O> SELF or(List<? extends ValueAccessPoint<?>> accessors, ConditionalOperator<O, ?> operator);
	
	interface OrderByChain<C, SELF extends OrderByChain<C, SELF>> {
		
		default SELF orderBy(SerializableFunction<C, ?> getter) {
			return orderBy(getter, Order.ASC);
		}
		
		default SELF orderBy(SerializableBiConsumer<C, ?> setter) {
			return orderBy(setter, Order.ASC);
		}
		
		default SELF orderBy(AccessorChain<C, ?> getter) {
			return orderBy(getter, Order.ASC);
		}
		
		SELF orderBy(SerializableFunction<C, ?> getter, Order order);
		
		SELF orderBy(SerializableBiConsumer<C, ?> setter, Order order);
		
		SELF orderBy(AccessorChain<C, ?> getter, Order order);
		
		SELF orderBy(AccessorChain<C, ?> getter, Order order, boolean ignoreCase);
		
		enum Order {
			ASC,
			DESC
		}
	}
	
	interface LimitAware<R> {
		
		R limit(int value);
		
		R limit(int value, Integer offset);
	}
	
	interface FluentOrderByClause<C, SELF extends FluentOrderByClause<C, SELF>> extends OrderByChain<C, SELF>, LimitAware<SELF> {
		
	}
	
	class CriteriaPath<IN, OUT> {
		
		private final List<ValueAccessPoint<?>> accessors;
		
		public CriteriaPath() {
			this.accessors = new ArrayList<>();
		}
		
		public CriteriaPath(SerializableFunction<IN, OUT> accessor) {
			this();
			this.accessors.add(Accessors.accessorByMethodReference(accessor));
		}
		
		public <S extends Collection<O>, O> CriteriaPath(SerializableCollectionFunction<IN, S, O> collectionAccessor, SerializableFunction<O, OUT> elementPropertyAccessor) {
			this();
			this.accessors.add(Accessors.accessorByMethodReference(collectionAccessor));
			this.accessors.add(Accessors.accessorByMethodReference(elementPropertyAccessor));
		}
		
		public List<ValueAccessPoint<?>> getAccessors() {
			return accessors;
		}
		
		public <NEXT> CriteriaPath<IN, NEXT> add(SerializableFunction<OUT, NEXT> accessor) {
			this.accessors.add(Accessors.accessorByMethodReference(accessor));
			return (CriteriaPath<IN, NEXT>) this;
		}
		
		public <NEXT, S extends Collection<NEXT>> CriteriaPath<IN, NEXT> add(SerializableCollectionFunction<OUT, S, NEXT> accessor) {
			this.accessors.add(Accessors.accessorByMethodReference(accessor));
			return (CriteriaPath<IN, NEXT>) this;
		}
	}
	
	@FunctionalInterface
	interface SerializableCollectionFunction<T, S extends Collection<O>, O> extends SerializableFunction<T, S> {
		
	}
}
