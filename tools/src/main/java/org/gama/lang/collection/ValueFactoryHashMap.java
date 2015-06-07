package org.gama.lang.collection;

import org.gama.lang.bean.IFactory;

import java.util.HashMap;

/**
 * Specialized {@link ValueFactoryMap} for HashMap.
 *
 * @author Guillaume Mary
 */
public class ValueFactoryHashMap<K, V> extends ValueFactoryMap<K, V> {

	public ValueFactoryHashMap(IFactory<K, V> factory) {
		super(new HashMap<K, V>(), factory);
	}

	public ValueFactoryHashMap(int initialCapacity, IFactory<K, V> factory) {
		super(new HashMap<K, V>(initialCapacity), factory);
	}

	public ValueFactoryHashMap(int initialCapacity, float loadFactor, IFactory<K, V> factory) {
		super(new HashMap<K, V>(initialCapacity, loadFactor), factory);
	}
}
