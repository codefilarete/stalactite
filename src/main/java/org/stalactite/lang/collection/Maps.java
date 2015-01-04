package org.stalactite.lang.collection;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mary
 */
public class Maps {
	
	public static <K, V> PutMethod<K, V> fastMap(K key, V value) {
		return new PutMethod<K, V>().put(key, value);
	}
	
	public static class PutMethod<K, V> {
		
		private Map<K, V> map;
		
		public PutMethod() {
			this(new HashMap<K, V>());
		}
		
		public PutMethod(Map<K, V> map) {
			this.map = map;
		}
		
		public PutMethod<K, V> put(K key, V value) {
			map.put(key, value);
			return this;
		}
		
		public Map<K, V> getMap() {
			return map;
		}
	}
}
