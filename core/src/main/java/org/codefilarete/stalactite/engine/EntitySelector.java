package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityCriteria.FluentOrderByClause;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.collection.Arrays;

/**
 * General contract for entity selectors
 *
 * @author Guillaume Mary
 */
public interface EntitySelector<C> {
	
	/**
	 * Creates a query which criteria targets mapped properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 *
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	ExecutableEntityQuery<C, ?> selectWhere();
	
	/**
	 * Creates a projection query which criteria targets mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of the query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link Selectable} added to the Select must have an alias</strong>.
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter);
	
	/**
	 * Creates a query with some criteria based on some properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyAccessor<C, O> getter, ConditionalOperator<O, ?> operator) {
		return selectWhere(AccessorChain.fromMethodReference(getter), operator);
	}
	
	/**
	 * Creates a query with some criteria based on some properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyMutator<C, O> setter, ConditionalOperator<O, ?> operator) {
		return selectWhere(Arrays.asList(Accessors.mutatorByMethodReference(setter)), operator);
	}
	
	/**
	 * Variation of {@link #selectWhere(SerializablePropertyAccessor, ConditionalOperator)} with a criteria on property of a property
	 * Please note that whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param getter1 a property accessor
	 * @param getter2 a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O, A> ExecutableEntityQuery<C, ?> selectWhere(SerializablePropertyAccessor<C, A> getter1, SerializablePropertyAccessor<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return selectWhere(AccessorChain.fromMethodReferences(getter1, getter2), operator);
	}
	
	/**
	 * Creates a query with some criteria based on some properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param accessorChain a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableEntityQuery<C, ?> selectWhere(List<? extends ValueAccessPoint<?>> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere().and(accessorChain, operator);
	}
	
	/**
	 * Creates a query with some criteria based on some properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param accessorChain a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableEntityQuery<C, ?> selectWhere(AccessorChain<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere(accessorChain.getAccessors(), operator);
	}
	
	default <O> ExecutableEntityQuery<C, ?> selectWhere(EntityCriteria.CriteriaPath<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere(accessorChain.getAccessors(), operator);
	}
	
	default <O, S extends Collection<O>, NEXT> ExecutableEntityQuery<C, ?> selectWhere(EntityCriteria.SerializableCollectionFunction<C, S, O> accessor1, SerializablePropertyAccessor<O, NEXT> accessor2, ConditionalOperator<NEXT, ?> operator) {
		return selectWhere(new EntityCriteria.CriteriaPath<>(accessor1).add(accessor2), operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of the query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyAccessor<C, O> getter, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter).and(getter, operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of the query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyMutator<C, O> setter, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter).and(setter, operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of the query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param getter1 a property accessor
	 * @param getter2 a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O, A> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializablePropertyAccessor<C, A> getter1, SerializablePropertyAccessor<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter, AccessorChain.fromMethodReferences(getter1, getter2).getAccessors(), operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of the query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if the targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param accessorChain a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter,
	                                                                  List<? extends ValueAccessPoint<?>> accessorChain,
	                                                                  ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter).and(accessorChain, operator);
	}
	
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter,
	                                                                  EntityCriteria.CriteriaPath<C, ?> accessorChain,
	                                                                  ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter, accessorChain.getAccessors(), operator);
	}
	
	default <O, S extends Collection<O>, NEXT> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter,
	                                                                                                 EntityCriteria.SerializableCollectionFunction<C, S, O> accessor1,
	                                                                                                 SerializablePropertyAccessor<O, NEXT> accessor2,
	                                                                                                 ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter, new EntityCriteria.CriteriaPath<>(accessor1).add(accessor2), operator);
	}
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 *
	 * @param <C> type of object returned by query execution
	 */
	interface ExecutableEntityQuery<C, SELF extends ExecutableEntityQuery<C, SELF>> extends EntityCriteria<C, SELF>, ExecutableQuery<C>, FluentOrderByClause<C, SELF> {
		
		SELF set(String paramName, Object paramValue);
		
		/**
		 * Overridden for a more accurate return type.
		 * {@inheritDoc}
		 */
		ExecutableEntityQuery<C, SELF> beginNested();
		
		/**
		 * Overridden for a more accurate return type.
		 * {@inheritDoc}
		 */
		ExecutableEntityQuery<C, SELF> endNested();
		
	}
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableProjection} to make an {@link EntityCriteria} executable
	 *
	 * @param <C> type of object returned by query execution
	 */
	interface ExecutableProjectionQuery<C, SELF extends ExecutableProjectionQuery<C, SELF>> extends EntityCriteria<C, SELF>, ExecutableProjection, FluentOrderByClause<C, SELF> {
		
		SELF set(String paramName, Object paramValue);
		
		/**
		 * Overridden for a more accurate return type.
		 * {@inheritDoc}
		 */
		ExecutableProjectionQuery<C, SELF> beginNested();
		
		/**
		 * Overridden for a more accurate return type.
		 * {@inheritDoc}
		 */
		ExecutableProjectionQuery<C, SELF> endNested();
	}
	
	/**
	 * Abstraction to configure the select clause of a query.
	 *
	 * @param <C> the type of context or entity for which the selection is configured
	 */
	interface SelectAdapter<C> {
		
		Set<Selectable<?>> getColumns();
		
		SelectAdapter<C> distinct();
		
		SelectAdapter<C> setDistinct(boolean distinct);
		
		SelectAdapter<C> add(Selectable<?> column);
		
		SelectAdapter<C> add(Selectable<?> column, String alias);
		
		default SelectAdapter<C> add(EntityCriteria.CriteriaPath<C, ?> property) {
			return add(property.getAccessors());
		}
		
		default SelectAdapter<C> add(EntityCriteria.CriteriaPath<C, ?> property, String alias) {
			return add(property.getAccessors(), alias);
		}
		
		default SelectAdapter<C> add(List<ReadWritePropertyAccessPoint<?, ?>> property) {
			return add(giveColumn(property));
		}
		
		default SelectAdapter<C> add(List<ReadWritePropertyAccessPoint<?, ?>> property, String alias) {
			return this.add(giveColumn(property), alias);
		}
		
		default Selectable<?> giveColumn(ReadWritePropertyAccessPoint<?, ?> property) {
			return giveColumn(Arrays.asList(property));
		}
		
		default Selectable<?> giveColumn(SerializablePropertyAccessor<C, ?> property) {
			return giveColumn(Accessors.readWriteAccessPoint(property));
		}
		
		default Selectable<?> giveColumn(SerializablePropertyMutator<C, ?> property) {
			return giveColumn(Accessors.readWriteAccessPoint(property));
		}
		
		Selectable<?> giveColumn(List<ReadWritePropertyAccessPoint<?, ?>> property);
	}
}
