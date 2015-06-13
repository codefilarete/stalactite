package org.gama.stalactite.persistence.sql;

import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;

/**
 * @author mary
 */
public class Dialect {
	
	private JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	private ColumnBinderRegistry columnBinderRegistry;
	
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(javaTypeToSqlTypeMapping, new ColumnBinderRegistry());
	}

	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry columnBinderRegistry) {
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
		this.columnBinderRegistry = columnBinderRegistry;
	}

	public JavaTypeToSqlTypeMapping getJavaTypeToSqlTypeMapping() {
		return javaTypeToSqlTypeMapping;
	}

	public ColumnBinderRegistry getColumnBinderRegistry() {
		return columnBinderRegistry;
	}
}
