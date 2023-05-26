package org.codefilarete.stalactite.engine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

/**
 * Contract for persisting entities.
 * 
 * @author Guillaume Mary
 */
public interface PersistExecutor<C> {
	
	void persist(Iterable<? extends C> entities);
	
	static <C, I> void persist(Iterable<? extends C> entities, ConfiguredPersister<C, I> persister) {
		persist(entities, persister::isNew, persister, persister, persister, persister::getId);
	}
	
	/**
	 * Persists given entities by choosing if they should be inserted or updated according to the given {@code isNewProvider} argument.
	 * 
	 * @param entities entities to be saved in database
	 * @param isNewProvider determines SQL operation to proceed
	 * @param selector used for entity update : loads entities from database so then can be compared to given ones and therefore compute their differences 
	 * @param updater executor of the update order
	 * @param inserter executor of the insert order
	 * @param idProvider used as a comparator between given entities and those found in database, hence avoid to rely on equals/hashcode mechanism of entities
	 * @param <C> entity type
	 * @param <I> entity identifier type
	 */
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
			// creating couples of modified and unmodified entities
			List<C> loadedEntities = selector.select(Iterables.collect(toUpdate, idProvider, HashSet::new));
			Map<I, C> loadedEntitiesPerId = Iterables.map(loadedEntities, idProvider);
			Map<I, C> modifiedEntitiesPerId = Iterables.map(toUpdate, idProvider);
			Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, loadedEntitiesPerId);
			List<Duo<C, C>> updateArg = new ArrayList<>();
			modifiedVSunmodified.forEach((k, v) -> updateArg.add(new Duo<>(k , v)));
			updater.update(updateArg, true);
		}
	}
	
	/**
	 * Persists given entities by choosing if they should be inserted or updated according to their presence in database.
	 * Dedicated to already-assigned use case : as a difference with {@link #persist(Iterable, ConfiguredPersister)} the {@code isNewProvider} can't
	 * rely on {@link ConfiguredPersister#isNew(Object)} because identifier is already set, so "isNew" can only be done through database checking.
	 *
	 * @param entities entities to be saved in database
	 * @param selector used for entity update : loads entities from database so then can be compared to given ones and therefore compute their differences 
	 * @param updater executor of the update order
	 * @param inserter executor of the insert order
	 * @param idProvider used as a comparator between given entities and those found in database, hence avoid to rely on equals/hashcode mechanism of entities
	 * @param <C> entity type
	 * @param <I> entity identifier type
	 */
	static <C, I> void persist(Iterable<? extends C> entities,
							   SelectExecutor<C, I> selector,
							   UpdateExecutor<C> updater,
							   InsertExecutor<C> inserter,
							   Function<C, I> idProvider) {
		if (Iterables.isEmpty(entities)) {
			return;
		}
		
		Set<I> entitiesIds = Iterables.collect(entities, idProvider::apply, HashSet::new);
		List<C> loadedEntities = selector.select(entitiesIds);
		Set<I> existingEntitiesIds = Iterables.collect(loadedEntities, idProvider::apply, HashSet::new);
		Predicate<C> isNewProvider = c -> !existingEntitiesIds.contains(idProvider.apply(c));
		persist(entities, isNewProvider, ids -> loadedEntities, updater, inserter, idProvider);
	}
}
