package org.stalactite.lang.collection;

import java.util.HashMap;
import java.util.Map;

import org.stalactite.lang.bean.IFactory;

/**
 * HashMap qui s'auto-alimente quand on lui demande une valeur qui n'existe pas. Il faut implémenter
 * {@link #createInstance)
 *
 * @author mary
 */
public abstract class EntryFactoryHashMap<K, V> extends HashMap<K, V> implements IFactory<K, V> {

	protected EntryFactoryHashMap() {
	}

	protected EntryFactoryHashMap(Map<? extends K, ? extends V> m) {
		super(m);
	}

	public EntryFactoryHashMap(int initialCapacity) {
		super(initialCapacity);
	}

	public EntryFactoryHashMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	@Override
	public V get(Object key) {
		// NB: toute requête avec autre chose qu'un K renverra null
		try {
			K typeKey = (K) key;
			V foundValue = super.get(key);
			if (foundValue == null) {
				foundValue = createInstance(typeKey);
				put(typeKey, foundValue);
			}
			return foundValue;
		} catch (ClassCastException cce) {
			return null;
		}
	}

}
