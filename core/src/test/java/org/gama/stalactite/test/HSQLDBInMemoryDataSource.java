package org.gama.stalactite.test;

import org.hsqldb.jdbc.JDBCDataSource;

/**
 * Simple DataSource HSQLDB pour les tests
 * 
 * @author mary
 */
public class HSQLDBInMemoryDataSource extends JDBCDataSource {
	
	public HSQLDBInMemoryDataSource() {
		// URL "aléatoire" pour éviter des percussions dans les tests
		setUrl("jdbc:hsqldb:mem:test"+hashCode());
		setUser("sa");
		setPassword("");
	}
}
