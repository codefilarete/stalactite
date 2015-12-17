package org.gama.reflection;

/**
 * @author Guillaume Mary
 */
public class AbstractReflector<C> {
	
	protected void handleException(Throwable t, C target, Object... args) {
		new ExceptionConverter().convertException(t, target, this, args);
	}
}
