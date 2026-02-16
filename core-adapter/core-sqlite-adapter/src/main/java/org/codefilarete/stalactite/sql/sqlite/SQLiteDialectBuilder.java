package org.codefilarete.stalactite.sql.sqlite;

import org.codefilarete.stalactite.engine.DialectBuilder;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectOptions;

/**
 * {@link Dialect} builder dedicated to SQLite
 * 
 * @author Guillaume Mary
 */
public class SQLiteDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultSQLiteDialect() {
		return new SQLiteDialectBuilder().build();
	}
	
	public SQLiteDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public SQLiteDialectBuilder(DialectOptions dialectOptions) {
		super(SQLiteDatabaseSettings.SQLITE_3_45, dialectOptions);
	}
}
