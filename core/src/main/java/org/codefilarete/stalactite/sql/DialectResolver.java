package org.codefilarete.stalactite.sql;

import java.sql.Connection;

import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * Simple contract to determine the {@link Dialect} to be used with a database
 * 
 * @author Guillaume Mary
 */
public interface DialectResolver {
	
	/**
	 * Expected to give the {@link Dialect} to be used with a database
	 * 
	 * @param conn an open connection to the database
	 * @return the most compatible dialect with given database connection
	 */
	Dialect determineDialect(Connection conn);
	
	/**
	 * @author Guillaume Mary
	 */
	interface DialectResolverEntry {
		
		DatabaseSignet getCompatibility();
		
		Dialect getDialect();
	}
}
