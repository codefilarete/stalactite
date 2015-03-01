package org.stalactite.persistence.sql;

import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.persistence.sql.dml.JDBCParameterBinder;

/**
 * @author mary
 */
public class Dialect {
	
	private JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	private JDBCParameterBinder jdbcParameterBinder;
	
	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this(javaTypeToSqlTypeMapping, new JDBCParameterBinder());
	}

	public Dialect(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, JDBCParameterBinder jdbcParameterBinder) {
		this.javaTypeToSqlTypeMapping = javaTypeToSqlTypeMapping;
		this.jdbcParameterBinder = jdbcParameterBinder;
	}

	public JavaTypeToSqlTypeMapping getJavaTypeToSqlTypeMapping() {
		return javaTypeToSqlTypeMapping;
	}

	public JDBCParameterBinder getJdbcParameterBinder() {
		return jdbcParameterBinder;
	}
}
