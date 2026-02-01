package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.EntityCriteria.CriteriaPath;
import org.codefilarete.stalactite.engine.EntityCriteria.FluentOrderByClause;
import org.codefilarete.stalactite.engine.EntityCriteria.SerializableCollectionFunction;
import org.codefilarete.stalactite.engine.listener.PersisterListener;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Experimental;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.PairIterator;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface EntityPersister<C, I> extends PersistExecutor<C>, InsertExecutor<C>, UpdateExecutor<C>, SelectExecutor<C, I>, DeleteExecutor<C, I>, PersisterListener<C, I> {
	
	/**
	 * Persists an instance either it is already persisted or not (insert or update).
	 *
	 * Check between insert or update is determined by id state which itself depends on identifier policy,
	 * see {@link SimpleIdMapping#IsNewDeterminer} implementations and
	 * {@link IdentifierInsertionManager} implementations for id value computation. 
	 *
	 * @param entity an entity to be persisted
	 * @throws StaleStateObjectException if updated row count differs from entities count
	 * @see #insert(Iterable)
	 * @see #update(Iterable, boolean)
	 */
	default void persist(C entity) {
		// determine insert or update operation
		persist(Collections.singleton(entity));
	}

	/**
	 * Choose either to insert or update entities according to their persistent state.
	 * 
	 * @param entities entities to be inserted or updated according to {@link #isNew(Object)} result
	 */
	void persist(Iterable<? extends C> entities);
	
	default void insert(C entity) {
		insert(Collections.singletonList(entity));
	}
	
	/**
	 * Updates an instance that may have changes.
	 * Groups statements to benefit from JDBC batch. Useful overall when allColumnsStatement
	 * is set to false.
	 *
	 * @param modified the supposing entity that has differences against {@code unmodified} entity
	 * @param unmodified the "original" (freshly loaded from database ?) entity
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be in
	 */
	default void update(C modified, C unmodified, boolean allColumnsStatement) {
		update(Collections.singletonList(new Duo<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Updates given entity in database according to following mechanism : it selects the existing data in database, then compares it with given
	 * entity in memory, and then updates database if necessary (nothing if no change was made).
	 *
	 * @param entity the entity to be updated
	 */
	default void update(C entity) {
		update(entity, true);
	}
	
	/**
	 * Updates given entity in database according to following mechanism : it selects the existing data in database, then compares it with given
	 * entity in memory, and then updates database if necessary (nothing if no change was made).
	 *
	 * @param entity the entity to be updated
	 * @param allColumnsStatement true to include all columns in statement even if only a part of them are touched, false to target only modified ones
	 */
	default void update(C entity, boolean allColumnsStatement) {
		update(entity, select(getId(entity)), allColumnsStatement);
	}
	
	/**
	 * Updates given entities in database according to following mechanism : it selects the existing data in database, then compares it with given
	 * entities in memory, and then updates database if necessary (nothing if no change was made).
	 * To be used for CRUD use case.
	 *
	 * @param entities the entities to be updated
	 */
	default void update(Iterable<C> entities) {
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Set<I> ids = Iterables.collect(entities, this::getId, KeepOrderSet::new);
		Set<C> entitiesFromDb = select(ids);
		// Given entities may not be in same order than loaded ones from DB, whereas order is required for comparison (else everything is different !)
		// so we join them by their id to make them match
		Map<C, I> idPerEntity = Iterables.map(entities, Function.identity(), this::getId, KeepOrderMap::new);
		Map<I, C> entityFromDbPerId = Iterables.map(entitiesFromDb, this::getId, Function.identity());
		Map<C, C> modifiedVsUnmodifiedEntities = Maps.innerJoinOnValuesAndKeys(idPerEntity, entityFromDbPerId, KeepOrderMap::new);
		update(() -> new PairIterator<>(modifiedVsUnmodifiedEntities.keySet(), modifiedVsUnmodifiedEntities.values()), true);
	}
	
	/**
	 * Helping method for "Command Design Pattern" so one can apply modifications to the entity loaded by its id without any concern of loading it.
	 * This implementation will load twice the entity, invoke given {@link Consumer} with one of them, and then call {@link #update(Object, Object, boolean)} afterward.
	 * Subclasses may override this behavior to enhance loading or change its algorithm (by using {@link #updateById(Iterable)} for instance)
	 * 
	 * @param id key of entity to be modified 
	 * @param entityConsumer business code expected to modify its given entity
	 */
	@Experimental
	default void update(I id, Consumer<C> entityConsumer) {
		update(Collections.singleton(id), entityConsumer);
	}
	
	/**
	 * Massive version of {@link #update(Object, Consumer)}. {@link Consumer} will be called for each found entities.
	 * 
	 * @param ids keys of entities to be modified
	 * @param entityConsumer business code expected to modify its given entity
	 */
	@Experimental
	default void update(Iterable<I> ids, Consumer<C> entityConsumer) {
		Set<C> unmodified = select(ids);
		Set<C> modified = select(ids);
		modified.forEach(entityConsumer);
		update(() -> new PairIterator<>(modified, unmodified), true);
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 *
	 * @param entity entity to be deleted
	 * @throws StaleStateObjectException if deleted row count differs from the entities count
	 */
	default void delete(C entity) {
		delete(Collections.singletonList(entity));
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimistic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entity entity to be deleted
	 */
	default void deleteById(C entity) {
		deleteById(Collections.singletonList(entity));
	}
	
	default C select(I id) {
		return Iterables.first(select(Collections.singleton(id)));
	}

	default Set<C> select(I... ids) {
		return select(Arrays.asHashSet(ids));
	}
	
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
	default <O> ExecutableEntityQuery<C, ?> selectWhere(SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return selectWhere(AccessorChain.fromMethodReference(getter), operator);
	}
	
	/**
	 * Creates a query with some criteria based on some properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
	 *
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableEntityQuery<C, ?> selectWhere(SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return selectWhere(Arrays.asList(Accessors.mutatorByMethodReference(setter)), operator);
	}
	
	/**
	 * Variation of {@link #selectWhere(SerializableFunction, ConditionalOperator)} with a criteria on property of a property
	 * Please note that whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
	 *
	 * @param getter1 a property accessor
	 * @param getter2 a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O, A> ExecutableEntityQuery<C, ?> selectWhere(SerializableFunction<C, A> getter1, SerializableFunction<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return selectWhere(AccessorChain.fromMethodReferences(getter1, getter2), operator);
	}
	
	/**
	 * Creates a query with some criteria based on some properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
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
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
	 *
	 * @param accessorChain a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableEntityQuery<C, ?> selectWhere(AccessorChain<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere(accessorChain.getAccessors(), operator);
	}
	
	default <O> ExecutableEntityQuery<C, ?> selectWhere(CriteriaPath<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectWhere(accessorChain.getAccessors(), operator);
	}
	
	default <O, S extends Collection<O>, NEXT> ExecutableEntityQuery<C, ?> selectWhere(SerializableCollectionFunction<C, S, O> accessor1, SerializableFunction<O, NEXT> accessor2, ConditionalOperator<NEXT, ?> operator) {
		return selectWhere(new CriteriaPath<>(accessor1, accessor2), operator);
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * Please note that the whole bean graph is loaded, not only entities that satisfy criteria.
	 *
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	ExecutableEntityQuery<C, ?> selectWhere();
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all column that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link org.codefilarete.stalactite.query.model.Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link org.codefilarete.stalactite.query.model.Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializableFunction<C, O> getter, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter).and(getter, operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link org.codefilarete.stalactite.query.model.Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link org.codefilarete.stalactite.query.model.Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializableBiConsumer<C, O> setter, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter).and(setter, operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link org.codefilarete.stalactite.query.model.Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link org.codefilarete.stalactite.query.model.Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param getter1 a property accessor
	 * @param getter2 a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O, A> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializableFunction<C, A> getter1, SerializableFunction<A, O> getter2, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter, AccessorChain.fromMethodReferences(getter1, getter2).getAccessors(), operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link org.codefilarete.stalactite.query.model.Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link org.codefilarete.stalactite.query.model.Selectable} added to the Select must have an alias</strong>.
	 * Raises an exception if targeted property is not mapped as a persisted one (transient).
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @param accessorChain a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, List<? extends ValueAccessPoint<?>> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter).and(accessorChain, operator);
	}
	
	default <O> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, CriteriaPath<C, ?> accessorChain, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter, accessorChain.getAccessors(), operator);
	}
	
	default <O, S extends Collection<O>, NEXT> ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter, SerializableCollectionFunction<C, S, O> accessor1, SerializableFunction<O, NEXT> accessor2, ConditionalOperator<O, ?> operator) {
		return selectProjectionWhere(selectAdapter, new CriteriaPath<>(accessor1, accessor2), operator);
	}
	
	/**
	 * Creates a projection query which criteria target mapped properties.
	 * {@link Select} must be modified by given select adapter (by default all columns that would allow to load the entity are present).
	 * User is expected to modify default {@link Select} by clearing it (optional) and add its {@link org.codefilarete.stalactite.query.model.Selectable}
	 * ({@link org.codefilarete.stalactite.sql.ddl.structure.Column} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}).
	 * Consumption and aggregation of query result is left to the user that must implement its {@link Accumulator}
	 * while executing the result of this method through {@link ExecutableProjection#execute(Accumulator)}.
	 * <strong>Note that all {@link org.codefilarete.stalactite.query.model.Selectable} added to the Select must have an alias</strong>.
	 *
	 * @param selectAdapter the {@link Select} clause modifier
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute(Accumulator)}
	 */
	ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter);
	
	Set<C> selectAll();
	
	boolean isNew(C entity);
	
	I getId(C entity);
	
	Class<C> getClassToPersist();
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
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
		
		default SelectAdapter<C> add(CriteriaPath<C, ?> property) {
			return add(property.getAccessors());
		}
		
		default SelectAdapter<C> add(CriteriaPath<C, ?> property, String alias) {
			return add(property.getAccessors(), alias);
		}
		
		default SelectAdapter<C> add(List<ValueAccessPoint<?>> property) {
			return add(giveColumn(property));
		}
		
		default SelectAdapter<C> add(List<ValueAccessPoint<?>> property, String alias) {
			return this.add(giveColumn(property), alias);
		}
		
		default Selectable<?> giveColumn(ValueAccessPoint<?> property) {
			return giveColumn(Arrays.asList(property));
		}
		
		default Selectable<?> giveColumn(SerializableFunction<C, ?> property) {
			return giveColumn(Accessors.accessorByMethodReference(property));
		}
		
		Selectable<?> giveColumn(List<ValueAccessPoint<?>> property);
	}
}
