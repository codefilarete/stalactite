package org.stalactite.lang.collection;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author mary
 */
public class Arrays {
	
	public static <T> List<T> asList(T... a) {
		return java.util.Arrays.asList(a);
	}
	
	public static <T> LinkedHashSet<T> asSet(T ... a) {
		return new LinkedHashSet<T>(asList(a));
	}
}
