package org.gama.lang.collection;

import java.util.HashMap;
import java.util.Map;

/**
 * Specialized {@link ValueFactoryMap} for HashMap.
 *
 * @author Guillaume Mary
 */
public abstract class ValueFactoryHashMap<K, V> extends ValueFactoryMap<K, V> {

	protected ValueFactoryHashMap() {
		super(new HashMap<K, V>());
	}

	protected ValueFactoryHashMap(Map<? extends K, ? extends V> m) {
		super(new HashMap<>(m));
	}

	public ValueFactoryHashMap(int initialCapacity) {
		super(new HashMap<K, V>(initialCapacity));
	}

	public ValueFactoryHashMap(int initialCapacity, float loadFactor) {
		super(new HashMap<K, V>(initialCapacity, loadFactor));
	}
}
