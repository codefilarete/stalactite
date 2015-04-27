package org.stalactite.reflection;

/**
 * @author Guillaume Mary
 */
public class AbstractReflector<C, T> {
	
	protected void handleException(Throwable t, C target, Object... args) {
		new ExceptionHandler<C>().handleException(t, target, this, args);
	}
}
