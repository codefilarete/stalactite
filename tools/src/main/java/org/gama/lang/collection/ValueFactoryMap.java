package org.gama.lang.collection;

import org.gama.lang.bean.IFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Map that self puts a newly created value when the requested ({@link #get(Object)}) key is not found (null is
 * returned. {@link #createInstance) must be implemented.
 * 
 * Acts as a wrapper for another concrete Map instance in order to have full control over Map storage algorithm.
 * 
 * @author Guillaume Mary
 */
public abstract class ValueFactoryMap<K, V> implements Map<K, V>, IFactory<K, V> {
	
	private final Map<K, V> delegate;
	
//	private final IFactory<K, V> factoryDelegate;
	
	/**
	 *
	 * @param delegate the wrapped instance
	 */
	public ValueFactoryMap(Map<K, V> delegate) {
		this.delegate = delegate;
	}
//	
//	/**
//	 *
//	 * @param delegate the wrapped instance
//	 * @param factoryDelegate the wrapped factory instance
//	 */
//	public ValueFactoryMap(Map<K, V> delegate, IFactory<K, V> factoryDelegate) {
//		this.delegate = delegate;
//		this.factoryDelegate = factoryDelegate;
//	}
	
	@Override
	public int size() {
		return delegate.size();
	}
	
	@Override
	public boolean isEmpty() {
		return delegate.isEmpty();
	}
	
	@Override
	public boolean containsKey(Object key) {
		return delegate.containsKey(key);
	}
	
	@Override
	public boolean containsValue(Object value) {
		return delegate.containsValue(value);
	}
	
	@Override
	public V get(Object key) {
		try {
			// NB: for everything else than a K instance requested, null will be returned
			K typeKey = (K) key;
			V foundValue = delegate.get(key);
			if (foundValue == null) {
				foundValue = createInstance(typeKey);
				put(typeKey, foundValue);
			}
			return foundValue;
		} catch (ClassCastException cce) {
			return null;
		}
	}
	
	@Override
	public V put(K key, V value) {
		return delegate.put(key, value);
	}
	
	@Override
	public V remove(Object key) {
		return delegate.remove(key);
	}
	
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		delegate.putAll(m);
	}
	
	@Override
	public void clear() {
		delegate.clear();
	}
	
	@Override
	public Set<K> keySet() {
		return delegate.keySet();
	}
	
	@Override
	public Collection<V> values() {
		return delegate.values();
	}
	
	@Override
	public Set<Entry<K, V>> entrySet() {
		return delegate.entrySet();
	}
}
