package org.codefilarete.stalactite.sql.sqlite.test;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.bean.Randomizer;
import org.sqlite.SQLiteDataSource;

/**
 * Simple Derby DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class SQLiteInMemoryDataSource extends UrlAwareDataSource {
	
	public SQLiteInMemoryDataSource() {
		// random URL to avoid conflict between tests
		this(Randomizer.INSTANCE.randomHexString(8));
	}
	
	private SQLiteInMemoryDataSource(String databaseName) {
		super("jdbc:sqlite:"+ databaseName);
		SQLiteDataSource delegate = new SQLiteDataSource();
		delegate.setDatabaseName(databaseName);
		// we enable the creation of the schema
		setDelegate(delegate);
	}
}
