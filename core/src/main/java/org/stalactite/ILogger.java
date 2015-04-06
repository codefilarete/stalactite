package org.stalactite;

/**
 * Interface simplifiée de log. Voir {@link Logger#getLogger(Class)}
 * 
 * @author mary
 */
public interface ILogger {

	void info(String message, Object ... args);

	void warn(String message, Object ... args);

	void error(String message, Object ... args);

	void debug(String message, Object ... args);

	void trace(String message, Object ... args);
}
