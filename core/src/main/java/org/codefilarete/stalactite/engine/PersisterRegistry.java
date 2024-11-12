package org.codefilarete.stalactite.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.ClassIterator;

/**
 * Contract for {@link BeanPersister} registry.
 * 
 * @author Guillaume Mary
 */
public interface PersisterRegistry {
	
	/**
	 * Returns the {@link BeanPersister} mapped for a class.
	 *
	 * @param clazz the class for which the {@link BeanPersister} must be given
	 * @param <C> the type of the persisted entity
	 * @return null if class has no persister registered
	 * @throws IllegalArgumentException if the class is not mapped
	 */
	<C, I> EntityPersister<C, I> getPersister(Class<C> clazz);
	
	/**
	 * Registers a {@link BeanPersister} on this instance. May overwrite an existing one
	 *
	 * @param persister any {@link BeanPersister}
	 * @param <C> type of persisted bean
	 */
	<C> void addPersister(EntityPersister<C, ?> persister);
	
	class DefaultPersisterRegistry implements PersisterRegistry {
		
		private final Map<Class<?>, EntityPersister> persisterCache = new HashMap<>();
		
		/**
		 * Looks for an {@link EntityPersister} registered for given class or one of its parent.
		 * Found persister is then capable of persisting any instance of given class.
		 * Made as such to take into account inheritance of a given entity and be capable of writing such code:
		 * <pre>{@code
		 * // this would work even if entity is a subtype of an aggregate root persister registered in the registry
		 * persisterRegistry.getPersister(entity.getClass()).insert(entity);
		 * }</pre>
		 *
		 * @param clazz the class for which the {@link EntityPersister} must be given
		 * @param <C> the type of the persisted entity
		 * @return null if class has no compatible persister registered
		 */
		@Override
		public <C, I> EntityPersister<C, I> getPersister(Class<C> clazz) {
			if (persisterCache.get(clazz) != null) {
				return persisterCache.get(clazz);
			} else {
				ClassIterator classIterator = new ClassIterator(clazz);
				EntityPersister<C, I> result;
				Class<?> pawn;
				do {
					pawn = classIterator.next();
					result = persisterCache.get(pawn);
				} while (result == null && classIterator.hasNext());
				// we add our finding to cache for future lookup
				if (result != null) {
					persisterCache.put(clazz, result);
				}
				return result;
			}
		}
		
		/**
		 * Registers a {@link EntityPersister} on this instance. May overwrite an existing one
		 *
		 * @param persister any {@link EntityPersister}
		 * @param <C> type of persisted bean
		 * @throws IllegalArgumentException if a persister already exists for class persisted by given persister
		 */
		@Override
		public <C> void addPersister(EntityPersister<C, ?> persister) {
			EntityPersister<C, ?> existingPersister = persisterCache.get(persister.getClassToPersist());
			if (existingPersister != null && existingPersister != persister) {
				throw new UnsupportedOperationException("Persister already exists for class " + Reflections.toString(persister.getClassToPersist()));
			}
			
			persisterCache.put(persister.getClassToPersist(), persister);
		}
		
		public Set<EntityPersister> getPersisters() {
			// copy the Set because values() is backed by the Map and getPersisters() is not expected to permit such modification
			return new HashSet<>(persisterCache.values());
		}
	}
	
}
