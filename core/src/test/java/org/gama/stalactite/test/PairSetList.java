package org.gama.stalactite.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.collection.PairIterator;

/**
 * Facility class for simple creation of List of Set of Pair. Main usage is unit test.
 * Pairs are instances of Map.Entry.
 *
 * @author Guillaume Mary
 */
public class PairSetList<K, V> {
	
	public static <K, V> List<Duo<K, V>> toPairs(Iterable<K> values1, Iterable<V> values2) {
		List<Duo<K, V>> indexValuePairs = new ArrayList<>();
		PairIterator<K, V> pairIterator = new PairIterator<>(values1, values2);
		while (pairIterator.hasNext()) {
			Duo<K, V> pair = pairIterator.next();
			indexValuePairs.add(pair);
		}
		return indexValuePairs;
	}
	
	private List<Set<Duo<K, V>>> toReturn = new ArrayList<>();
	
	private Set<Duo<K, V>> current = new HashSet<>();
	
	public PairSetList() {
		toReturn.add(current);
	}
	
	/**
	 * Add key-value to the current Set
	 * @param k a key
	 * @param v a value
	 * @return this
	 */
	public PairSetList<K, V> add(K k, V v) {
		current.add(new Duo<>(k, v));
		return this;
	}
	
	/**
	 * Create a new Set in the list and add key-value to it
	 * @param k a key
	 * @param v a value
	 * @return this
	 */
	public PairSetList<K, V> of(K k, V v) {
		toReturn.add(current = new HashSet<>());
		return add(k, v);
	}
	
	public List<Set<Duo<K, V>>> asList() {
		return toReturn;
	}
}
