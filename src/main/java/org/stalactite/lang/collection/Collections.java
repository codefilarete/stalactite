package org.stalactite.lang.collection;

import java.util.Collection;
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

	
}
