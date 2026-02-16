package org.codefilarete.stalactite.sql.mariadb;

import org.codefilarete.stalactite.engine.DialectBuilder;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectOptions;

/**
 * {@link Dialect} builder dedicated to MariaDB
 * 
 * @author Guillaume Mary
 */
public class MariaDBDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultMariaDBDialect() {
		return new MariaDBDialectBuilder().build();
	}
	
	public MariaDBDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public MariaDBDialectBuilder(DialectOptions dialectOptions) {
		super(MariaDBDatabaseSettings.MARIADB_10_0, dialectOptions);
	}
}
