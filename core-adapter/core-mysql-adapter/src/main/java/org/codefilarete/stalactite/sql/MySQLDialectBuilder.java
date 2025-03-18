package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DialectBuilder;

/**
 * {@link Dialect} builder dedicated to MySQL
 * 
 * @author Guillaume Mary
 */
public class MySQLDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultMySQLDialect() {
		return new MySQLDialectBuilder().build();
	}
	
	public MySQLDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public MySQLDialectBuilder(DialectOptions dialectOptions) {
		super(MySQLDatabaseSettings.MYSQL_5_6, dialectOptions);
	}
}
