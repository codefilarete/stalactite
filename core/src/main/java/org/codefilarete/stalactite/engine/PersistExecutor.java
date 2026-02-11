package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

/**
 * Defines the contract for persisting entities by automatically determining whether to insert or update them
 * based on the entity's presence in the database.
 * 
 * @param <C> The type of entities to be persisted.
 */
public interface PersistExecutor<C> {
	
	void persist(Iterable<? extends C> entities);
	
	
	/**
	 * Shortcut for {@link #persist(Iterable)} that avoids {@link Iterable} creation.
	 *
	 * @param entities an array of entities of type C to be persisted
	 */
	default void persist(C... entities) {
		persist(Arrays.asSet(entities));
	}
	
	static <C, I> PersistExecutor<C> forPersister(ConfiguredPersister<C, I> persister) {
		IdentifierInsertionManager<C, I> identifierInsertionManager = persister.getMapping().getIdMapping().getIdentifierInsertionManager();
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager
				&& ((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getIsPersistedFunction() == null) {
			// if the "IsPersistedFunction" is not null, we'll produce a DefaultPersistExecutor because it bases
			// its algorithm on persister::isNew which is supplied by the IsPersistedFunction
			return new AlreadyAssignedIdentifierPersistExecutor<>(persister);
		} else {
			return new DefaultPersistExecutor<>(persister);
		}
	}
	
	/**
	 * Implementation for already-assigned identifier policies that doesn't provide persisted-state-management lambdas.
	 * Algorithm is based on a database query that retrieves existing entities to determine if they should be inserted
	 * or updated.
	 * The counter-part of it versus {@link DefaultPersistExecutor} is that it queries the database for all given
	 * entities, not only modified ones. Hence, it creates some heavier back-and-forth with the database which creates
	 * a database and a memory overload.
	 */
	class AlreadyAssignedIdentifierPersistExecutor<C, I> implements PersistExecutor<C> {
		
		protected final EntityPersister<C, I> persister;
		
		public AlreadyAssignedIdentifierPersistExecutor(EntityPersister<C, I> persister) {
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
			persist(entities, persister, persister, persister, persister::getId);
		}
		
		/**
		 * Persists given entities by choosing if they should be inserted or updated according to a database query
		 * that retrieved already persisted entities.
		 *
		 * @param entities entities to be saved in the database
		 * @param selector used for entity update: loads entities from the database so they can be compared to given ones and therefore compute their differences
		 * @param updater executor of the update order
		 * @param inserter executor of the insert order
		 * @param idProvider used as a comparator between given entities and those found in the database, hence it avoids to rely on equals/hashcode mechanism of entities
		 */
		protected void persist(Iterable<? extends C> entities,
							   SelectExecutor<C, I> selector,
							   UpdateExecutor<C> updater,
							   InsertExecutor<C> inserter,
							   Function<C, I> idProvider) {
			if (Iterables.isEmpty(entities)) {
				return;
			}
			// determine insert or update operation
			Map<I, C> existingEntitiesPerId = Iterables.map(selector.select(Iterables.collect(entities, idProvider, HashSet::new)), idProvider);
			Map<I, C> modifiedEntitiesPerId = Iterables.stream(entities)
					.filter(c -> existingEntitiesPerId.containsKey(idProvider.apply(c)))
					.collect(Collectors.toMap(idProvider, Function.identity(), (k1, k2) -> k1));
			Collection<C> toUpdate = modifiedEntitiesPerId.values();
			Collection<C> toInsert = Iterables.stream(entities)
					.filter(c -> !existingEntitiesPerId.containsKey(idProvider.apply(c)))
					.collect(Collectors.toSet());
			if (!toInsert.isEmpty()) {
				inserter.insert(toInsert);
			}
			if (!toUpdate.isEmpty()) {
				// creating couples of modified and unmodified entities
				Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, existingEntitiesPerId);
				Set<Duo<C, C>> updateArg = Iterables.collect(modifiedVSunmodified.entrySet(),
						entry -> new Duo<>(entry.getKey(), entry.getValue()),
						LinkedHashSet::new);
				updater.update(updateArg, true);
			}
		}
	}
	
	/**
	 * Default implementation of {@link PersistExecutor} that delegates persistence operations to the underlying
	 * {@link EntityPersister}. Determines whether to insert or update entities based on
	 * {@link EntityPersister#isNew(Object)}.
	 * 
	 * @param <C> the entity type to persist
	 * @param <I> the entity identifier type
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
		 * Insert, Update, and Select operations are delegated to the given {@link EntityPersister}
		 *
		 * @param entities entities to be saved in the database
		 */
		@Override
		public void persist(Iterable<? extends C> entities) {
			persist(entities, new DefaultIsNewDeterminer<C>() {
				@Override
				public boolean isNew(C c) {
					return persister.isNew(c);
				}
			}, persister, persister, persister, persister::getId);
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
							   NewEntitiesCollector<C> isNewProvider,
							   SelectExecutor<C, I> selector,
							   UpdateExecutor<C> updater,
							   InsertExecutor<C> inserter,
							   Function<C, I> idProvider) {
			if (Iterables.isEmpty(entities)) {
				return;
			}
			// determine insert or update operation
			Set<C> toInsert = isNewProvider.collectNewEntities(entities);
			Set<C> toUpdate = Iterables.minus(Iterables.asList(entities), toInsert);
			if (!toInsert.isEmpty()) {
				inserter.insert(toInsert);
			}
			if (!toUpdate.isEmpty()) {
				// creating couples of modified and unmodified entities
				Map<I, C> existingEntitiesPerId = Iterables.map(selector.select(Iterables.collect(toUpdate, idProvider, HashSet::new)), idProvider);
				Map<I, C> modifiedEntitiesPerId = Iterables.map(toUpdate, idProvider);
				Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, existingEntitiesPerId);
				Set<Duo<C, C>> updateArg = Iterables.collect(modifiedVSunmodified.entrySet(),
						entry -> new Duo<>(entry.getKey(), entry.getValue()),
						LinkedHashSet::new);
				updater.update(updateArg, true);
			}
		}
		
		/**
		 * Small contract to determine if an entity is persisted or not
		 * @param <T>
		 */
		public interface NewEntitiesCollector<T> {
			
			Set<T> collectNewEntities(Iterable<? extends T> entities);
		}
		
		/**
		 * Implementation of {@link NewEntitiesCollector} that delegates to {@link #isNew(Object)} for each entity
		 * 
		 * @param <T>
		 * @author Guillaume Mary
		 */
		public abstract static class DefaultIsNewDeterminer<T> implements NewEntitiesCollector<T> {
			
			public Set<T> collectNewEntities(Iterable<? extends T> entities) {
				return Iterables.stream(entities).filter(this::isNew).collect(Collectors.toSet());
			}
			
			/**
			 * @param t an entity
			 * @return true if the entity doesn't exist in database
			 */
			public abstract boolean isNew(T t);
		}
	}
}
