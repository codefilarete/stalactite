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
	
	/**
	 * Choose either to insert or update entities according to their persistent state.
	 *
	 * @param entities entities to be inserted or updated according to {@link EntityPersister#isNew(Object)} result
	 */
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
		return forPersister(persister, persister);
	}
	
	static <C, I> PersistExecutor<C> forPersister(EntityWriteExecutor<C, I> writer, EntityReadExecutor<C, I> reader) {
		IdentifierInsertionManager<C, I> identifierInsertionManager = writer.getMapping().getIdMapping().getIdentifierInsertionManager();
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager
				&& ((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getIsPersistedFunction() == null) {
			// if the "IsPersistedFunction" is not null, we'll produce a DefaultPersistExecutor because it bases
			// its algorithm on persister::isNew which is supplied by the IsPersistedFunction
			return new AlreadyAssignedIdentifierPersistExecutor<>(writer, reader);
		} else {
			return new DefaultPersistExecutor<>(writer, reader);
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
		
		private final EntityWriteExecutor<C, I> writer;
		private final EntityReadExecutor<C, I> reader;
		
		public AlreadyAssignedIdentifierPersistExecutor(EntityWriteExecutor<C, I> writer, EntityReadExecutor<C, I> reader) {
			this.writer = writer;
			this.reader = reader;
		}
		
		/**
		 * Persists given entities by choosing if they should be inserted or updated according to the given {@link EntityPersister#isNew(Object)} argument.
		 * Insert, Update and Select operation are delegated to given {@link EntityPersister}
		 *
		 * @param entities entities to be saved in the database
		 */
		@Override
		public void persist(Iterable<? extends C> entities) {
			if (Iterables.isEmpty(entities)) {
				return;
			}
			// determine insert or update operation
			Map<I, C> existingEntitiesPerId = Iterables.map(reader.select(Iterables.collect(entities, writer::getId, HashSet::new)), writer::getId);
			Map<I, C> modifiedEntitiesPerId = Iterables.stream(entities)
					.filter(c -> existingEntitiesPerId.containsKey(writer.getId(c)))
					.collect(Collectors.toMap(writer::getId, Function.identity(), (k1, k2) -> k1));
			Collection<C> toUpdate = modifiedEntitiesPerId.values();
			Collection<C> toInsert = Iterables.stream(entities)
					.filter(c -> !existingEntitiesPerId.containsKey(writer.getId(c)))
					.collect(Collectors.toSet());
			if (!toInsert.isEmpty()) {
				writer.insert(toInsert);
			}
			if (!toUpdate.isEmpty()) {
				// creating couples of modified and unmodified entities
				Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, existingEntitiesPerId);
				Set<Duo<C, C>> updateArg = Iterables.collect(modifiedVSunmodified.entrySet(),
						entry -> new Duo<>(entry.getKey(), entry.getValue()),
						LinkedHashSet::new);
				writer.update(updateArg, true);
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
		 * The {@link EntityWriteExecutor} to delegate SQL operations to.
		 */
		private final EntityWriteExecutor<C, I> writer;
		private final EntityReadExecutor<C, I> reader;
		
		public DefaultPersistExecutor(EntityWriteExecutor<C, I> writer, EntityReadExecutor<C, I> reader) {
			this.writer = writer;
			this.reader = reader;
		}
		
		/**
		 * Persists given entities by choosing if they should be inserted or updated according to the given {@link EntityPersister#isNew(Object)} argument.
		 * Insert, Update, and Select operations are delegated to the given {@link EntityPersister}
		 *
		 * @param entities entities to be saved in the database
		 */
		@Override
		public void persist(Iterable<? extends C> entities) {
			if (Iterables.isEmpty(entities)) {
				return;
			}
			// determine insert or update operation
			Set<C> toInsert = Iterables.collect(entities, writer::isNew, Function.identity(), HashSet::new);
			Set<C> toUpdate = Iterables.minus(Iterables.asList(entities), toInsert);
			if (!toInsert.isEmpty()) {
				writer.insert(toInsert);
			}
			if (!toUpdate.isEmpty()) {
				// creating couples of modified and unmodified entities
				Map<I, C> existingEntitiesPerId = Iterables.map(reader.select(Iterables.collect(toUpdate, writer::getId, HashSet::new)), writer::getId);
				Map<I, C> modifiedEntitiesPerId = Iterables.map(toUpdate, writer::getId);
				Map<C, C> modifiedVSunmodified = Maps.innerJoin(modifiedEntitiesPerId, existingEntitiesPerId);
				Set<Duo<C, C>> updateArg = Iterables.collect(modifiedVSunmodified.entrySet(),
						entry -> new Duo<>(entry.getKey(), entry.getValue()),
						LinkedHashSet::new);
				writer.update(updateArg, true);
			}
		}
	}
}
