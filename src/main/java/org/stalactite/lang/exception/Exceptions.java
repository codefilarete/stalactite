package org.stalactite.lang.exception;

/**
 * 
 * 
 */
public abstract class Exceptions {
	
	/**
	 * Faite en sorte de toujours lever une RuntimeException même si <tt>t</tt>
	 * n'en est pas une.
	 * @param t 
	 * @throws <tt>t</tt> si c'est une RuntimeException, sinon une nouvelle RuntimeException avec <tt>t</tt> en tant que cause
	 */
	public static void throwAsRuntimeException(Throwable t) {
		if (t instanceof RuntimeException) {
			throw (RuntimeException) t;
		} else {
			throw new RuntimeException(t);
		}
	}
	
}
