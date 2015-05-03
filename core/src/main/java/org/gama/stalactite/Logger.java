package org.gama.stalactite;

import org.slf4j.LoggerFactory;

/**
 * Implémentation simple basée sur SLF4J.
 * Il est attendu d'utiliser {@link #getLogger(Class)} pour obtenir un ILogger.
 * 
 * @author mary
 */
public class Logger implements ILogger {
	
	public static Logger getLogger(Class clazz) {
		return new Logger(LoggerFactory.getLogger(clazz));
	}

	private final org.slf4j.Logger delegate;

	private Logger(org.slf4j.Logger delegate) {
		this.delegate = delegate;
	}

	@Override
	public void info(String message, Object ... args) {
		delegate.info(message, args);
	}

	@Override
	public void warn(String message, Object... args) {
		delegate.warn(message, args);
	}

	@Override
	public void error(String message, Object ... args) {
		delegate.error(message, args);
	}

	@Override
	public void debug(String message, Object ... args) {
		delegate.debug(message, args);
	}
	
	@Override
	public void trace(String message, Object ... args) {
		delegate.trace(message, args);
	}
}
