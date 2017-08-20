package org.gama.sql.result;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.function.ThrowingSupplier;

/**
 * Simple class to ease access or creation to entity from the cache.
 * A cache organized per bean class, then per bean identifier.
 *
 * @see #computeIfAbsent(Class, Object, ThrowingSupplier)
 */
public final class SimpleBeanCache {
	
	/** Surrogate bean cache : per type, then per identifier */
	private final Map<Class, Map<Object /* bean key */, Object>> entityCache;
	
	public SimpleBeanCache() {
		this(new HashMap<>());
	}
	
	public SimpleBeanCache(Map<Class, Map<Object, Object>> entityCache) {
		this.entityCache = entityCache;
	}
	
	/**
	 * Clears the cache
	 */
	public void clear() {
		this.entityCache.clear();
	}
	
	/**
	 * Main class that tries to retrieve an entity by its class and identifier or instanciates it and put it into the cache
	 *
	 * @param clazz the type of the entity
	 * @param identifier the identifier of the entity (Long, String, ...), not null (because null has no purpose here)
	 * @param factory the "method" that will be called to create the entity when the entity is not in the cache
	 * @return the existing instance in the cache or a new object
	 */
	public <E extends Throwable, T> T computeIfAbsent(Class<T> clazz, Object identifier, ThrowingSupplier<Object /* identifier */, E> factory) throws E {
		Object rowInstance = entityCache.computeIfAbsent(clazz, k -> new HashMap<>()).get(identifier);
		if (rowInstance == null) {
			rowInstance = factory.get();
			entityCache.computeIfAbsent(clazz, k -> new HashMap<>()).put(identifier, rowInstance);
		}
		return (T) rowInstance;
	}
}