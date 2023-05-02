package org.codefilarete.stalactite.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

/**
 * @author Guillaume Mary
 */
public interface PersistExecutor<C> {
	
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
			// creating couples of modified and unmodified entities
			List<C> loadedEntities = selector.select(toUpdate.stream().map(idProvider).collect(Collectors.toList()));
			Map<I, C> loadedEntitiesPerId = Iterables.map(loadedEntities, idProvider);
			Map<I, C> modifiedEntitiesPerId = Iterables.map(toUpdate, idProvider);
			Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, loadedEntitiesPerId);
			List<Duo<C, C>> updateArg = new ArrayList<>();
			modifiedVSunmodified.forEach((k, v) -> updateArg.add(new Duo<>(k , v)));
			updater.update(updateArg, true);
		}
	}
}
