package org.stalactite.lang;

/**
 * @author guillaume.mary
 */
public abstract class Strings {
	
	public static boolean isEmpty(CharSequence charSequence) {
		return charSequence == null || charSequence.length() == 0;
	}
	
	private Strings() {}
}
