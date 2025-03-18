package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DialectBuilder;

/**
 * {@link Dialect} builder dedicated to PostgreSQL
 * 
 * @author Guillaume Mary
 */
public class PostgreSQLDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultPostgreSQLDialect() {
		return new PostgreSQLDialectBuilder().build();
	}
	
	public PostgreSQLDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public PostgreSQLDialectBuilder(DialectOptions dialectOptions) {
		super(PostgreSQLDatabaseSettings.POSTGRESQL_9_6, dialectOptions);
	}
}
