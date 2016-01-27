package org.gama.lang.bean;

/**
 * @author Guillaume Mary
 */
public class Objects {
	
	/**
	 * Vérifie que o1.equals(o2) en prenant en compte les cas où o1 ou o2 sont null.
	 *
	 * @param o1 un objet
	 * @param o2 un objet
	 * @return true si o1 == null && o2 == null
	 *		<br> o1.equals(o2) si o1 et o2 non null
	 *		<br> false si o1 != null ou-exclusif o2 != null
	 */
	public static boolean equalsWithNull(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return true;
		} else if (o1 != null ^ o2 != null) {
			return false;
		} else {
			return o1.equals(o2);
		}
	}
	
	public static <T, U> boolean equalsWithNull(T t, U u, BiPredicate<T, U> equalsNonNullDelegate) {
		if (t == null && u == null) {
			return true;
		} else if (t != null ^ u != null) {
			return false;
		} else {
			return equalsNonNullDelegate.test(t, u);
		}
	}
	
	public static <E> E preventNull(E value, E nullValue) {
		return value == null ? nullValue : value;
	}
	
	/** To be replaced by Java 8 BiPredicate */
	public interface BiPredicate<T, U> {
		boolean test(T t, U u);
	}
	
	/** To be replaced by Java 8 Function */
	public interface Function<T, R> {
		R apply(T t);
	}
}
