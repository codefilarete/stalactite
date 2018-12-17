package org.gama.stalactite.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.PairIterator;

/**
 * Facility class for simple creation of List of Set of pairs. Main usage is unit test.
 * Pairs are instances of {@link Duo}.
 *
 * @author Guillaume Mary
 */
public class PairSetList<K, V> {
	
	public static <K, V> List<Duo<? extends K, ? extends V>> toPairs(Iterable<K> values1, Iterable<V> values2) {
		PairIterator<K, V> pairIterator = new PairIterator<>(values1, values2);
		return Iterables.copy(pairIterator);
	}
	
	private List<Set<Duo<K, V>>> toReturn = new ArrayList<>();
	
	private Set<Duo<K, V>> current = new HashSet<>();
	
	public PairSetList() {
		toReturn.add(current);
	}
	
	/**
	 * Adds key-value to the current Set
	 * 
	 * @param k a key
	 * @param v a value
	 * @return this
	 */
	public PairSetList<K, V> add(K k, V v) {
		current.add(new Duo<>(k, v));
		return this;
	}
	
	/**
	 * Creates a new Set in the list and add key-value to it
	 * 
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
