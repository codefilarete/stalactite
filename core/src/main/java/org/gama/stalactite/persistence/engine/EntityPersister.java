package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.Experimental;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.collection.PairIterator;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

/**
 * @author Guillaume Mary
 */
public interface EntityPersister<C, I> extends InsertExecutor<C>, UpdateExecutor<C>, SelectExecutor<C, I>, DeleteExecutor<C, I>, PersisterListener<C, I> {
	
	/**
	 * Persists an instance either it is already persisted or not (insert or update).
	 *
	 * Check between insert or update is determined by id state which itself depends on identifier policy,
	 * see {@link org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy#IsNewDeterminer} implementations and
	 * {@link org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager} implementations for id value computation. 
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
	
	void persist(Iterable<? extends C> entities);
	
	
	static <C, I> void persist(Iterable<? extends C> entities,
							   Predicate<C> isNewProvider,
							   SelectExecutor<C, I> selector,
							   UpdateExecutor<C> updater,
							   InsertExecutor<C> inserter,
							   Function<C, I> idProvider) {
		if (Iterables.isEmpty(entities)) {
			return;
		}
		// determine insert or update operation
		List<C> toInsert = new ArrayList<>(20);
		List<C> toUpdate = new ArrayList<>(20);
		for (C c : entities) {
			if (isNewProvider.test(c)) {
				toInsert.add(c);
			} else {
				toUpdate.add(c);
			}
		}
		if (!toInsert.isEmpty()) {
			inserter.insert(toInsert);
		}
		if (!toUpdate.isEmpty()) {
			// creating couple of modified and unmodified entities
			List<C> loadedEntities = selector.select(toUpdate.stream().map(idProvider).collect(Collectors.toList()));
			Map<I, C> loadedEntitiesPerId = Iterables.map(loadedEntities, idProvider);
			Map<I, C> modifiedEntitiesPerId = Iterables.map(toUpdate, idProvider);
			Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, loadedEntitiesPerId);
			List<Duo<C, C>> updateArg = new ArrayList<>();
			modifiedVSunmodified.forEach((k, v) -> updateArg.add(new Duo<>(k , v)));
			updater.update(updateArg, true);
		}
	}
	
	default void insert(C entity) {
		insert(Collections.singletonList(entity));
	}
	
	/**
	 * Updates an instance that may have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 *
	 * @param modified the supposing entity that has differences againt {@code unmodified} entity
	 * @param unmodified the "original" (freshly loaded from database ?) entity
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be in
	 */
	default void update(C modified, C unmodified, boolean allColumnsStatement) {
		update(Collections.singletonList(new Duo<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Updates given entity in database according to following mecanism : it selects the existing data in database, then compares it with given
	 * entity in memory, and then updates database if necessary (nothing if no change was made).
	 *
	 * @param entity the entity to be updated
	 */
	default void update(C entity) {
		update(entity, true);
	}
	
	/**
	 * Updates given entity in database according to following mecanism : it selects the existing data in database, then compares it with given
	 * entity in memory, and then updates database if necessary (nothing if no change was made).
	 *
	 * @param entity the entity to be updated
	 * @param allColumnsStatement true to include all columns in statement even if only a part of them are touched, false to target only modified ones
	 */
	default void update(C entity, boolean allColumnsStatement) {
		update(entity, select(getId(entity)), allColumnsStatement);
	}
	
	/**
	 * Updates given entities in database according to following mecanism : it selects the existing data in database, then compares it with given
	 * entities in memory, and then updates database if necessary (nothing if no change was made).
	 * To be used for CRUD use case.
	 *
	 * @param entities the entities to be updated
	 */
	default void update(Iterable<C> entities) {
		List<I> ids = Iterables.collect(entities, this::getId, ArrayList::new);
		List<C> entitiesFromDb = select(ids);
		// Given entities may not be in same order than loaded ones from DB, whereas order is required for comparison (else everything is different !)
		// so we join them by their id to make them match
		Map<C, I> idPerEntity = Iterables.map(entities, Function.identity(), this::getId);
		Map<I, C> entityFromDbPerId = Iterables.map(entitiesFromDb, this::getId, Function.identity());
		Map<C, C> modifiedVsUnmodifiedEntities = Maps.innerJoinOnValuesAndKeys(idPerEntity, entityFromDbPerId);
		update(() -> new PairIterator<>(modifiedVsUnmodifiedEntities.keySet(), modifiedVsUnmodifiedEntities.values()), true);
	}
	
	/**
	 * Helping method for "Command Design Pattern" so one can apply modifications to the entity loaded by its id without any concern of loading it.
	 * This implementation will load twice the entity, invoke given {@link Consumer} with one of them, and then call {@link #update(Object, Object, boolean)} afterward.
	 * Subclasses may override this behavior to enhance loading or change its algorithm (by using {@link #updateById(Iterable)} for instance)
	 * 
	 * @param id key of entity to be modified 
	 * @param entityConsumer businness code expected to modify its given entity
	 */
	@Experimental
	default void update(I id, Consumer<C> entityConsumer) {
		update(Collections.singleton(id), entityConsumer);
	}
	
	/**
	 * Massive version of {@link #update(Object, Consumer)}. {@link Consumer} will be called for each found entities.
	 * 
	 * @param ids keys of entities to be modified
	 * @param entityConsumer businness code expected to modify its given entity
	 */
	@Experimental
	default void update(Iterable<I> ids, Consumer<C> entityConsumer) {
		List<C> unmodified = select(ids);
		List<C> modified = select(ids);
		modified.forEach(entityConsumer);
		update(() -> new PairIterator<>(modified, unmodified), true);
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 *
	 * @param entity entity to be deleted
	 * @throws StaleStateObjectException if deleted row count differs from entities count
	 */
	default void delete(C entity) {
		delete(Collections.singletonList(entity));
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entity entity to be deleted
	 */
	default void deleteById(C entity) {
		deleteById(Collections.singletonList(entity));
	}
	
	default C select(I id) {
		return Iterables.first(select(Collections.singleton(id)));
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * Please note that whole bean graph is loaded, not only entities that satisfie criteria.
	 *
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	<O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * Please note that whole bean graph is loaded, not only entities that satisfie criteria.
	 *
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	<O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
	
	List<C> selectAll();
	
	boolean isNew(C entity);
	
	I getId(C entity);
	
	Class<C> getClassToPersist();
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 * @param <C> type of object returned by query execution
	 */
	interface ExecutableEntityQuery<C> extends EntityCriteria<C>, ExecutableQuery<C> {
		
	}
	
	/**
	 * Contract that allows to create some query criteria based on property accessors
	 * 
	 * @param <C> type of object returned by query execution
	 */
	interface EntityCriteria<C> {
		
		/**
		 * Combines with "and" given criteria on property  
		 *
		 * @param getter a method reference to a getter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws IllegalArgumentException if column matching getter was not found
		 */
		<O> EntityCriteria<C> and(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		/**
		 * Combines with "and" given criteria on property  
		 *
		 * @param setter a method reference to a setter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws IllegalArgumentException if column matching setter was not found
		 */
		<O> EntityCriteria<C> and(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
		
		/**
		 * Combines with "or" given criteria on property  
		 *
		 * @param getter a method reference to a getter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws IllegalArgumentException if column matching getter was not found
		 */
		<O> EntityCriteria<C> or(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		/**
		 * Combines with "or" given criteria on property  
		 *
		 * @param setter a method reference to a setter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws IllegalArgumentException if column matching setter was not found
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
		 * @throws IllegalArgumentException if column matching getter was not found
		 */
		<A, B> EntityCriteria<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
	}
}
