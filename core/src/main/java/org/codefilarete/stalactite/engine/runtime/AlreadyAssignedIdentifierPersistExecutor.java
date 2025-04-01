package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.codefilarete.stalactite.engine.InsertExecutor;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.UpdateExecutor;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

/**
 * {@link PersistExecutor} which persists entities according to their presence in database.
 * Dedicated to already-assigned use case because identifier is already set, so "isNew" can only be done through database checking.
 * 
 * @param <C> entity type
 * @param <I> entity identifier type
 * @author Guillaume Mary
 */
public class AlreadyAssignedIdentifierPersistExecutor<C, I> implements PersistExecutor<C> {
	
	private final ConfiguredPersister<C, I> persister;
	
	public AlreadyAssignedIdentifierPersistExecutor(ConfiguredPersister<C, I> persister) {
		this.persister = persister;
	}
	
	/**
	 * Implementation based on database presence checking
	 * @param entities entities to be saved in database
	 */
	@Override
	public void persist(Iterable<? extends C> entities) {
		SelectExecutor<C, I> selector = persister;
		UpdateExecutor<C> updater = persister;
		InsertExecutor<C> inserter = persister;
		Function<C, I> idProvider = persister.getMapping()::getId;
		
		Map<I, ? extends C> entitiesPerId = Iterables.map(entities, idProvider);
		Set<C> existingEntities = selector.select(entitiesPerId.keySet());
		Set<I> existingEntitiesIds = Iterables.collect(existingEntities, idProvider, HashSet::new);
		Predicate<C> isNewProvider = c -> !existingEntitiesIds.contains(idProvider.apply(c));
		
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
			Map<I, C> loadedEntitiesPerId = Iterables.map(existingEntities, idProvider);
			Map<I, C> modifiedEntitiesPerId = Iterables.map(toUpdate, idProvider);
			Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, loadedEntitiesPerId);
			List<Duo<C, C>> updateArg = new ArrayList<>();
			modifiedVSunmodified.forEach((k, v) -> updateArg.add(new Duo<>(k , v)));
			updater.update(updateArg, true);
		}
	}
}
