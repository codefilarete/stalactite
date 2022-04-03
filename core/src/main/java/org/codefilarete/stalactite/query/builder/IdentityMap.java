package org.codefilarete.stalactite.query.builder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Storage of key-value pairs based on key {@link System#identityHashCode} to avoid loss of bean in classical {@link Map} : because those are based on
 * instance hashCode, beans can't be retrieved due some hashCode change when computation is based on incompletly filled attributes, such as collection.
 * <p>
 * It does not implement {@link Map} because it is mainly used as a marking name instead of the anonymous Map class which only instantiation brings
 * the implementation : by this name the developer clearly says its intention (and should add a comment why such a Map is required in its
 * algorithm ;) ).
 * It could also have been replaced by {@link java.util.IdentityHashMap} (or use it internally) but, first it would have broken previous principle,
 * and overall one have to know that {@link java.util.IdentityHashMap} compares keys on their {@link System#identityHashCode} but also its values
 * which, beyond being not really intuitive, is not required in production code, and brings some difficulties in tests (because even same Strings are
 * different with '=='). 
 * 
 * @param <K> key type
 * @param <V> value type
 */
public class IdentityMap<K, V> {
	
	private final Map<Integer, V> delegate;
	
	public IdentityMap() {
		this.delegate = new HashMap<>();
	}
	
	public IdentityMap(int capacity) {
		this.delegate = new HashMap<>(capacity);
	}
	
	public void put(K key, V value) {
		this.delegate.put(System.identityHashCode(key), value);
	}
	
	public V get(K key) {
		return this.delegate.get(System.identityHashCode(key));
	}
	
	public Collection<V> values() {
		return delegate.values();
	}
	
	/**
	 * Exposes internal storage, made for testing purpose, not expected to be used in production
	 * @return internal storage
	 */
	public Map<Integer, V> getDelegate() {
		return delegate;
	}
	
	/**
	 * Implemented for easier debug
	 *
	 * @return delegate's toString()
	 */
	@Override
	public String toString() {
		return delegate.toString();
	}
}
