package org.gama.lang.collection;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

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
	
	/**
	 * Method that can be used as a method reference for array index access. Will call the {@link Supplier} in case of index that is out of the array
	 * boundaries.
	 *
	 * @param index index of the array to return
	 * @param defaultValue the value to return when boundaires are reached (negative index or higher than array length)
	 * @param <C> type of the array to use
	 * @return a function that can be used as a reference for array index acccess
	 */
	public static <C> Function<C[], C> get(int index, Supplier<C> defaultValue) {
		return cs -> isOutOfBounds(index, cs) ? defaultValue.get() : cs[index];
	}
	
	private static <C> boolean isOutOfBounds(int index, C[] cs) {
		return index < 0 || index > cs.length-1;
	}
	
	public static <C> C first(C[] args) {
		return args[0];
	}
	
	public static <C> C last(C[] args) {
		return args[args.length-1];
	}
}
