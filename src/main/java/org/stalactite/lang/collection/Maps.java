package org.stalactite.lang.collection;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author mary
 */
public final class Maps {
	
	/**
	 * Méthode qui permet d'enchaîner les appels à add(K, V) et ainsi de créer rapidement une Map d'éléments constants.
	 * @return une Map
	 */
	public static <K, V> ChainingMap<K, V> asMap(K key, V value) {
		return new ChainingMap<K, V>().add(key, value);
	}
	
	/**
	 * Simple {@link LinkedHashMap} qui permet d'enchaîner les appels à {@link #add(Object, Object)} (équivalent de put)
	 * et donc de créer rapidement une {@link Map} d'éléments.
	 * @param <K>
	 * @param <V>
	 */
	public static class ChainingMap<K, V> extends LinkedHashMap<K, V> {
		
		public ChainingMap() {
			super();
		}
		
		public ChainingMap<K, V> add(K key, V value) {
			put(key, value);
			return this;
		}
	}
}
