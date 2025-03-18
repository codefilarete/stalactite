package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DialectBuilder;

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
