package org.gama.sql;

import java.sql.Connection;

/**
 * Simple interface for who needs a Connection.
 * 
 * @author Guillaume Mary
 */
public interface IConnectionProvider {
	
	Connection getConnection();
}
