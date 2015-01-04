package org.stalactite.lang.bean;

/**
 * @author mary
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
}
