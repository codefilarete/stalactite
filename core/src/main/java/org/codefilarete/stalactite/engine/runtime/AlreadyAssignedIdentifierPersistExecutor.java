package org.codefilarete.stalactite.engine.runtime;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.PersistExecutor.DefaultPersistExecutor;
import org.codefilarete.tool.collection.Iterables;

/**
 * {@link PersistExecutor} which persists entities according to their presence in the database.
 * Dedicated to the already-assigned use case because the identifier is already set, so "isNew" can only be done through database checking.
 * 
 * @param <C> entity type
 * @param <I> entity identifier type
 * @author Guillaume Mary
 */
public class AlreadyAssignedIdentifierPersistExecutor<C, I> extends DefaultPersistExecutor<C, I> {
	
	public AlreadyAssignedIdentifierPersistExecutor(ConfiguredPersister<C, I> persister) {
		super(persister);
	}
	
	/**
	 * Implementation based on database presence checking
	 * @param entities entities to be saved in the database
	 */
	@Override
	public void persist(Iterable<? extends C> entities) {
		Function<C, I> idProvider = persister::getId;
		// Computing the isNewProvider by getting entities from the database because there's no other way to make it since ids are already-assigned
		Set<I> entitiesIds = Iterables.collect(entities, idProvider, HashSet::new);
		Map<I, C> existingEntitiesPerId = Iterables.map(persister.select(entitiesIds), idProvider);
		Predicate<C> isNewProvider = c -> !existingEntitiesPerId.containsKey(idProvider.apply(c));
		
		super.persist(entities,
				isNewProvider,
				// We get the entities from the Set that we got from the database earlier for more efficiency
				ids -> {
					List<I> list = Iterables.copy(ids);
					return Iterables.collect(existingEntitiesPerId.entrySet(), entry -> list.contains(entry.getKey()), Entry::getValue, HashSet::new);
				},
				persister,
				persister,
				idProvider);
	}
}
