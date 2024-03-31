package org.codefilarete.stalactite.sql.statement.binder;

import java.math.BigDecimal;

import org.codefilarete.stalactite.sql.ddl.DefaultTypeMapping;

/**
 * @author Guillaume Mary
 */
public class SQLiteTypeMapping extends DefaultTypeMapping {
	
	public SQLiteTypeMapping() {
		super();
		// SQLite is very lax about typing, the best way to make it support BigDecimal is to say it to store it as text (not bigint)
		put(BigDecimal.class, "text");
	}
}