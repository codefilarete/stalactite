package org.stalactite.benchmark;

import java.util.Date;

import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;

/**
 * @author Guillaume Mary
 */
public class DefaultTypeMapping extends JavaTypeToSqlTypeMapping {
	
	public DefaultTypeMapping() {
		super();
		put(Boolean.class, "bit");
		put(Boolean.TYPE, "bit");
		put(Double.class, "double");
		put(Double.TYPE, "double");
		put(Float.class, "float");
		put(Float.TYPE, "float");
		put(Long.class, "bigint");
		put(Long.TYPE, "bigint");
		put(Integer.class, "integer");
		put(Integer.TYPE, "integer");
		put(Date.class, "timestamp");
		put(String.class, "varchar");
		put(String.class, 16383, "varchar($l)");
	}
}
