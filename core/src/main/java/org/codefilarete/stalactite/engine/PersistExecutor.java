package org.codefilarete.stalactite.engine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

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
	
	/**
	 * A default {@link PersistExecutor} that points to {@link #persist(Iterable, Predicate, SelectExecutor, UpdateExecutor, InsertExecutor, Function)}
	 * @param <C>
	 * @author Guillaume Mary
	 */
	class DefaultPersistExecutor<C, I> implements PersistExecutor<C> {
		
		/**
		 * The {@link EntityPersister} to delegate SQL operations to.
		 */
		protected final EntityPersister<C, I> persister;
		
		public DefaultPersistExecutor(EntityPersister<C, I> persister) {
			this.persister = persister;
		}
		
		/**
		 * Persists given entities by choosing if they should be inserted or updated according to the given {@link EntityPersister#isNew(Object)} argument.
		 * Insert, Update and Select operation are delegated to given {@link EntityPersister}
		 *
		 * @param entities entities to be saved in the database
		 */
		@Override
		public void persist(Iterable<? extends C> entities) {
			persist(entities, persister::isNew, persister, persister, persister, persister::getId);
		}
		
		/**
		 * Persists given entities by choosing if they should be inserted or updated according to the given {@code isNewProvider} argument.
		 *
		 * @param entities entities to be saved in the database
		 * @param isNewProvider determines SQL operation to proceed
		 * @param selector used for entity update: loads entities from the database so they can be compared to given ones and therefore compute their differences
		 * @param updater executor of the update order
		 * @param inserter executor of the insert order
		 * @param idProvider used as a comparator between given entities and those found in the database, hence it avoids to rely on equals/hashcode mechanism of entities
		 */
		protected void persist(Iterable<? extends C> entities,
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
				Map<I, C> existingEntitiesPerId = Iterables.map(selector.select(Iterables.collect(toUpdate, idProvider, HashSet::new)), idProvider);
				Map<I, C> modifiedEntitiesPerId = Iterables.map(toUpdate, idProvider);
				Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, existingEntitiesPerId);
				List<Duo<C, C>> updateArg = new ArrayList<>();
				modifiedVSunmodified.forEach((k, v) -> updateArg.add(new Duo<>(k, v)));
				updater.update(updateArg, true);
			}
		}
	}
}
