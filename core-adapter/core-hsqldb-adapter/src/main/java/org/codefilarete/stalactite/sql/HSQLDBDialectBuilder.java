package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DialectBuilder;

/**
 * {@link Dialect} builder dedicated to HSQLDB
 * 
 * @author Guillaume Mary
 */
public class HSQLDBDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultHSQLDBDialect() {
		return new HSQLDBDialectBuilder().build();
	}
	
	public HSQLDBDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public HSQLDBDialectBuilder(DialectOptions dialectOptions) {
		super(HSQLDBDatabaseSettings.HSQLDB_2_7, dialectOptions);
	}
}
