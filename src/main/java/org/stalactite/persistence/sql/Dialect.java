package org.stalactite.persistence.sql;

import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.persistence.sql.dml.binder.ParameterBinderRegistry;

/**
 * @author mary
 */
public class Dialect {
	
	private JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	private ParameterBinderRegistry parameterBinderRegistry;
	
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(javaTypeToSqlTypeMapping, new ParameterBinderRegistry());
	}

	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ParameterBinderRegistry parameterBinderRegistry) {
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
		this.parameterBinderRegistry = parameterBinderRegistry;
	}

	public JavaTypeToSqlTypeMapping getJavaTypeToSqlTypeMapping() {
		return javaTypeToSqlTypeMapping;
	}

	public ParameterBinderRegistry getParameterBinderRegistry() {
		return parameterBinderRegistry;
	}
}
