package org.stalactite.lang.collection;

import java.util.*;

/**
 * @author mary
 */
public class Arrays {
	
	public static <T> List<T> asList(T... a) {
		return java.util.Arrays.asList(a);
	}
	
	public static <T> LinkedHashSet<T> asSet(T ... a) {
		LinkedHashSet<T> toReturn = new LinkedHashSet<>(a.length, 1);
		java.util.Collections.addAll(toReturn, a);
		return toReturn;
	}
}
