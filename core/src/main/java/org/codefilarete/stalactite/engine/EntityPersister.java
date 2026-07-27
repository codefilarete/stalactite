package org.codefilarete.stalactite.engine;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Experimental;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.PairIterator;

/**
 * General contract for entity persisters
 * 
 * @author Guillaume Mary
 */
public interface EntityPersister<C, I> extends EntityReadWriteExecutor<C, I>, EntitySelector<C> {
	
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
	
	Set<C> selectAll();
	
//	I getId(C entity);
	
	Class<C> getClassToPersist();
	
}
