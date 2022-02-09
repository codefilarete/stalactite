package org.codefilarete.stalactite.sql;

import javax.annotation.Nonnull;
import java.sql.Connection;

/**
 * A simple contract to give the eventually existing {@link Connection}
 * 
 * @author Guillaume Mary
 */
public interface ConnectionProvider {
	
	/**
	 * Gives an eventually existing {@link Connection} or opens a new one if it doesn't exist or current one is closed.
	 * 
	 * @return neither null nor a closed connection
	 */
	@Nonnull
	Connection giveConnection();
}
