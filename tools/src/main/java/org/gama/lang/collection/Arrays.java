package org.gama.lang.collection;

import java.util.*;

/**
 * @author Guillaume Mary
 */
public class Arrays {
	
	public static <T> List<T> asList(T... a) {
		return new ArrayList<>(java.util.Arrays.asList(a));
	}
	
	public static <T> LinkedHashSet<T> asSet(T ... a) {
		LinkedHashSet<T> toReturn = new LinkedHashSet<>(a.length, 1);
		java.util.Collections.addAll(toReturn, a);
		return toReturn;
	}
	
	public static boolean isEmpty(Object[] array) {
		return array == null || array.length == 0;
	}
}
