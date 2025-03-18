package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DialectBuilder;

/**
 * {@link Dialect} builder dedicated to H2
 * 
 * @author Guillaume Mary
 */
public class H2DialectBuilder extends DialectBuilder {
	
	public static Dialect defaultH2Dialect() {
		return new H2DialectBuilder().build();
	}
	
	public H2DialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public H2DialectBuilder(DialectOptions dialectOptions) {
		super(H2DatabaseSettings.H2_1_4, dialectOptions);
	}
}
