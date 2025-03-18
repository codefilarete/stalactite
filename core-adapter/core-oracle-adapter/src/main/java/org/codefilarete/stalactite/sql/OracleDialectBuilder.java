package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DialectBuilder;

/**
 * {@link Dialect} builder dedicated to Oracle
 * 
 * @author Guillaume Mary
 */
public class OracleDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultOracleDialect() {
		return new OracleDialectBuilder().build();
	}
	
	public OracleDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public OracleDialectBuilder(DialectOptions dialectOptions) {
		super(OracleDatabaseSettings.ORACLE_23_0, dialectOptions);
	}
}
