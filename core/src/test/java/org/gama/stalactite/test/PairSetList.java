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
 * @param <K> left element type of {@link Duo}s, "key"
 * @param <V> right element type of {@link Duo}s, "value"
 * @author Guillaume Mary
 */
public class PairSetList<K, V> {
	
	/**
	 * Equivalent to {@code new PairSetList<>(k, v)}
	 * 
	 * @param k any value
	 * @param v any value
	 * @param <K> "key" type
	 * @param <V> "value" type
	 * @return a new {@link PairSetList}
	 */
	public static <K, V> PairSetList<K, V> pairSetList(K k, V v) {
		return new PairSetList<>(k, v);
	}
	
	public static <K, V> List<Duo<? extends K, ? extends V>> toPairs(Iterable<K> values1, Iterable<V> values2) {
		PairIterator<K, V> pairIterator = new PairIterator<>(values1, values2);
		return Iterables.copy(pairIterator);
	}
	
	private List<Set<Duo<K, V>>> toReturn = new ArrayList<>();
	
	private Set<Duo<K, V>> current = new HashSet<>();
	
	/**
	 * Default constructor.
	 * It adds an empty new row, so one can directly calls {@link #add(Object, Object)} on it.
	 */
	public PairSetList() {
		toReturn.add(current);
	}
	
	/**
	 * Constructor that adds a  new row and fills it with the given parameters by calling {@link #add(Object, Object)}. 
	 */
	public PairSetList(K k, V v) {
		this();
		add(k, v);
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
	public PairSetList<K, V> newRow(K k, V v) {
		return newRow().add(k, v);
	}
	
	/**
	 * Creates a new Set in the list
	 *
	 * @return this
	 */
	public PairSetList<K, V> newRow() {
		toReturn.add(current = new HashSet<>());
		return this;
	}
	
	public List<Set<Duo<K, V>>> asList() {
		return toReturn;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PairSetList<?, ?> that = (PairSetList<?, ?>) o;
		return toReturn.equals(that.toReturn);
	}
	
	@Override
	public int hashCode() {
		return toReturn.hashCode();
	}
	
	@Override
	public String toString() {
		return toReturn.toString();
	}
}
