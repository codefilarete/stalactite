package org.gama.lang.collection;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;

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
	
	/**
	 * Method that can be used as a method reference for array index access.
	 * 
	 * @param index index of the array to return
	 * @param <C> type of the array to use
	 * @return a function that can be used as a reference for array index acccess
	 */
	public static <C> Function<C[], C> get(int index) {
		return cs -> cs[index];
	}
	
	public static <C> C first(C[] args) {
		return args[0];
	}
	
	public static <C> C last(C[] args) {
		return args[args.length-1];
	}
}
