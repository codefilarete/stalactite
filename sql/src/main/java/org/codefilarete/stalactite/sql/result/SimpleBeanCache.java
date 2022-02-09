package org.codefilarete.stalactite.sql.result;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Simple class to ease access or creation to entity from the cache.
 * A cache organized per bean class, then per bean identifier.
 *
 * @see #computeIfAbsent(Class, Object, Function)
 */
public final class SimpleBeanCache {
	
	/** Surrogate bean cache : per type, then per identifier */
	private final Map<Class, Map<BeanKey /* bean key */, Object>> entityCache;
	
	public SimpleBeanCache() {
		this(new HashMap<>());
	}
	
	public SimpleBeanCache(Map<Class, Map<BeanKey, Object>> entityCache) {
		this.entityCache = entityCache;
	}
	
	/**
	 * Clears the cache
	 */
	public void clear() {
		this.entityCache.clear();
	}
	
	/**
	 * Main method that tries to retrieve an entity by its class and identifier or instanciates it and put it into the cache
	 *
	 * @param clazz the type of the entity
	 * @param identifier the identifier of the entity (Long, String, ...),
	 * 				not null (because null has no purpose here), can be an array (for composed key case)
	 * @param factory the "method" that will be called to create the entity when the entity is not in the cache
	 * @return the existing instance in the cache or a new object
	 */
	public <C, I> C computeIfAbsent(Class<C> clazz, @Nonnull I identifier, Function<I, C> factory) {
		BeanKey key;
		if (identifier.getClass().isArray()) {
			// NB: we must cast into Object[] to avoid the JVM to wrap the identifier into an array of Object due to varargs constructor
			key = new BeanKey((Object[]) identifier);
		} else {
			key = new BeanKey(identifier);
		}
		C rowInstance = (C) entityCache.computeIfAbsent(clazz, k -> new HashMap<>()).get(key);
		if (rowInstance == null) {
			rowInstance = factory.apply(identifier);
			entityCache.computeIfAbsent(clazz, k -> new HashMap<>()).put(key, rowInstance);
		}
		return rowInstance;
	}
	
	/**
	 * Wrapper for {@link Map} key to take arrays into account : array's hashcode are not computed on their content (as the opposit of List)
	 * hence two arrays having same content are not under the same hashcode. This class computes hashcode arrays on their content, thus, two
	 * beans with same composed keys hit the cache.
	 */
	private static class BeanKey {
		
		private final Object[] keys;
		/** array hashCode, introduced for optimization: prevent from computing it at each hashCode() method call */
		private final int hashCode;
		
		private BeanKey(Object ... keys) {
			this.keys = keys;
			this.hashCode = Arrays.hashCode(keys);;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof BeanKey)) return false;
			BeanKey beanKey = (BeanKey) o;
			return Arrays.equals(keys, beanKey.keys);
		}
		
		@Override
		public int hashCode() {
			return hashCode;
		}
	}
}