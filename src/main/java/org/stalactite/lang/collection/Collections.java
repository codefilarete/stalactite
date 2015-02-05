package org.stalactite.lang.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author mary
 */
public class Collections {
	
	/**
	 * Renvoie true si la collection est null ou vide
	 * 
	 * @param c
	 * @return true si la collection est null ou vide
	 */
	public static boolean isEmpty(Collection c) {
		return c == null || c.isEmpty();
	}

	/**
	 * Renvoie true si la Map est null ou vide
	 *
	 * @param m
	 * @return true si la Map est null ou vide
	 */
	public static boolean isEmpty(Map m) {
		return m == null || m.isEmpty();
	}

	public static <E> List<E> cat(Collection<E> ... collections) {
		List<E> toReturn = new ArrayList<>(collections.length * 10);	// arbitrary size, ArrayList.addAll will adapt
		for (Collection<E> collection : collections) {
			toReturn.addAll(collection);
		}
		return toReturn;
	}
}
