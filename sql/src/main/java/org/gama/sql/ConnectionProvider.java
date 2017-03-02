package org.gama.sql;

import java.sql.Connection;

/**
 * @author Guillaume Mary
 */
public interface ConnectionProvider {
	
	Connection getCurrentConnection();
}
