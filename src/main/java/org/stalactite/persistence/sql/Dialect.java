package org.stalactite.persistence.sql;

import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;

/**
 * @author mary
 */
public class Dialect {
	
	private JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
	}
	
	public JavaTypeToSqlTypeMapping getJavaTypeToSqlTypeMapping() {
		return javaTypeToSqlTypeMapping;
	}
}
