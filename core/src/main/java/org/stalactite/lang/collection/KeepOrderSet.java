package org.stalactite.lang.collection;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 * Faux Set qui doit conserver l'ordre d'insertion
 * 
 * @author mary
 */
public class KeepOrderSet<E> implements Iterable<E> {
	
	private LinkedHashSet<E> delegate = new LinkedHashSet<>();
	
	public KeepOrderSet(E ... e) {
		this.delegate.addAll(Arrays.asList(e));
	}
	
	public int size() {
		return delegate.size();
	}
	
	public boolean isEmpty() {
		return delegate.isEmpty();
	}
	
	public boolean contains(Object o) {
		return delegate.contains(o);
	}
	
	public Iterator<E> iterator() {
		return delegate.iterator();
	}
	
	public boolean add(E e) {
		return delegate.add(e);
	}
	
	public boolean containsAll(Collection<?> c) {
		return delegate.containsAll(c);
	}
	
	public boolean addAll(Collection<? extends E> c) {
		return delegate.addAll(c);
	}
	
	public boolean equals(Object o) {
		return o instanceof KeepOrderSet && delegate.equals(o);
	}
	
	public int hashCode() {
		return delegate.hashCode();
	}
	
	/**
	 * Renvoie l'équivalent de cette instance sous la forme d'un vrai Set (cloné)
	 * @return le contenu de cette instance dans un LinkedHashSet
	 */
	public LinkedHashSet<E> asSet() {
		return new LinkedHashSet<>(delegate);
	}
}
