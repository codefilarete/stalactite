package org.gama.stalactite.test;

import org.gama.lang.collection.PairIterator;

import java.util.*;

/**
 * Facility class for simple creation of Pairs. Main usage is test unit.
 * Pairs are instances of Map.Entry.
 * 
 * @author Guillaume Mary
 */
public class PairSet<K, V> {
	
	public static <K, V> PairSet<K,V> of(K k, V v) {
		return new PairSet<K, V>().add(k, v);
	}
	
	public static <K, V> LinkedHashSet<Map.Entry<K, V>> toPairs(Iterable<K> values1, Iterable<V> values2) {
		LinkedHashSet<Map.Entry<K, V>> indexValuePairs = new LinkedHashSet<>();
		PairIterator<K, V> pairIterator = new PairIterator<>(values1, values2);
		while (pairIterator.hasNext()) {
			Map.Entry<K, V> pair = pairIterator.next();
			indexValuePairs.add(new AbstractMap.SimpleEntry<>(pair.getKey(), pair.getValue()));
		}
		return indexValuePairs;
	}
	
	private Set<Map.Entry<K, V>> toReturn = new LinkedHashSet<>();
	
	public PairSet<K, V> add(K k, V v) {
		toReturn.add(new AbstractMap.SimpleEntry<>(k, v));
		return this;
	}
	
	public Set<Map.Entry<K, V>> asSet() {
		return toReturn;
	}
}
