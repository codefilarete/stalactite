package org.gama.stalactite.benchmark;

import org.gama.stalactite.persistence.engine.PersistenceContext;

/**
 * @author Guillaume Mary
 */
public class PersistenceContexts {
	
	private static final ThreadLocal<PersistenceContext> CURRENT_CONTEXT = new ThreadLocal<>();
	
	public static PersistenceContext getCurrent() {
		PersistenceContext currentContext = CURRENT_CONTEXT.get();
		if (currentContext == null) {
			throw new IllegalStateException("No context found for current thread");
		}
		return currentContext;
	}
	
	public static void setCurrent(PersistenceContext context) {
		CURRENT_CONTEXT.set(context);
	}
	
	public static void clearCurrent() {
		CURRENT_CONTEXT.remove();
	}
}
