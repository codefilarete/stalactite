package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DialectBuilder;

/**
 * {@link Dialect} builder dedicated to Derby
 * 
 * @author Guillaume Mary
 */
public class DerbyDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultDerbyDialect() {
		return new DerbyDialectBuilder().build();
	}
	
	public DerbyDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public DerbyDialectBuilder(DialectOptions dialectOptions) {
		super(DerbyDatabaseSettings.DERBY_10_14, dialectOptions);
	}
}
