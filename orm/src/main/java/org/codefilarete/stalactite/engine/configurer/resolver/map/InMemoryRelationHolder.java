package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.codefilarete.tool.Nullable.nullable;

class InMemoryRelationHolder<I, K, V> {
	
	/**
	 * In memory and temporary Map storage.
	 */
	private final ThreadLocal<Map<I, Set<MapEntry>>> relationCollectionPerEntity = new ThreadLocal<>();
	
	void storeRelation(I source, K key, V value) {
		Set<MapEntry> relatedDuos = giveRelatedDuos(source);
		MapEntry result = new MapEntry(key, value);
		relatedDuos.add(result);
	}
	
	void storeRelation(I source, K key, V value, Integer index) {
		Set<MapEntry> relatedDuos = giveRelatedDuos(source);
		MapEntry result = new MapEntry(key, value, index);
		relatedDuos.add(result);
	}
	
	private Set<MapEntry> giveRelatedDuos(I source) {
		Map<I, Set<MapEntry>> srcidcMap = relationCollectionPerEntity.get();
		return srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
	}
	
	Collection<MapEntry> giveEntityEntries(I src) {
		Map<I, Set<MapEntry>> currentMap = relationCollectionPerEntity.get();
		return nullable(currentMap)
				.map(map -> map.get(src))
				.get();
	}
	
	public void init() {
		this.relationCollectionPerEntity.set(new HashMap<>());
	}
	
	public void clear() {
		this.relationCollectionPerEntity.remove();
	}
	
	class MapEntry {
		
		private K left;
		private V right;
		private Integer index;
		
		public MapEntry(K left, V right) {
			this.left = left;
			this.right = right;
		}
		
		public MapEntry(K left, V right, Integer index) {
			this(left, right);
			this.index = index;
		}
		
		public void setLeft(K left) {
			this.left = left;
		}
		
		public K getLeft() {
			return left;
		}
		
		public void setRight(V right) {
			this.right = right;
		}
		
		public V getRight() {
			return right;
		}
		
		public Integer getIndex() {
			return index;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			MapEntry mapEntry = (MapEntry) o;
			return Objects.equals(left, mapEntry.left);
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(left);
		}
	}
}
