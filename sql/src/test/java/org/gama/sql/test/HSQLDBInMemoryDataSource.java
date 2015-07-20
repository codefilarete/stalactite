package org.gama.sql.test;

import org.gama.sql.UrlAwareDataSource;
import org.hsqldb.jdbc.JDBCDataSource;

import java.util.Random;

/**
 * Simple DataSource HSQLDB pour les tests
 * 
 * @author Guillaume Mary
 */
public class HSQLDBInMemoryDataSource extends UrlAwareDataSource {
	
	public HSQLDBInMemoryDataSource() {
		super("jdbc:hsqldb:mem:test" + Integer.toHexString(new Random().nextInt()));
		JDBCDataSource delegate = new JDBCDataSource();
		// URL "aléatoire" pour éviter des percussions dans les tests
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
	
	@Override
	public String toString() {
		return getUrl();
	}
}
