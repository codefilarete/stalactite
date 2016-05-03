package org.gama.stalactite.persistence.engine;

import java.sql.Connection;

/**
 * @author Guillaume Mary
 */
public interface ConnectionProvider {
	
	Connection getCurrentConnection();
}
