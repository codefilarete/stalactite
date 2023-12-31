package org.codefilarete.stalactite.sql.ddl;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;

/**
 * A registry that let one specifies SQL types for Java types and {@link Column}s.
 * This is used for schema generation.
 * One can specify the SQL type of a particular {@link Column} with {@link #put(Column, String)}, as well as one for
 * a Java class with {@link #put(Class, String)}. The latter will act as a fallback if an SQL type is not found for a
 * {@link Column} : its {@link Column#getJavaType() Java type} will be checked for SQL type presence.
 * Designed as a wrapper of {@link JavaTypeToSqlTypeMapping} by adding {@link Column} type registration.
 * 
 * See {@link org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry} for default readers and writers
 * to database.
 * 
 * @author Guillaume Mary
 * @see #getTypeName(Column)
 */
public class SqlTypeRegistry {
	
	private final JavaTypeToSqlTypeMapping javaTypeToSqlType;
	
	private final Map<Column, String> columnToSQLType = new HashMap<>();
	
	public SqlTypeRegistry() {
		this(new JavaTypeToSqlTypeMapping());
	}
	
	public SqlTypeRegistry(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping) {
		this.javaTypeToSqlType = javaTypeToSqlTypeMapping;
	}
	
	public JavaTypeToSqlTypeMapping getJavaTypeToSqlTypeMapping() {
		return javaTypeToSqlType;
	}
	
	/**
	 * Registers a Java class to a SQL type mapping
	 *
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
	 */
	public void put(Class clazz, String sqlType) {
		javaTypeToSqlType.put(clazz, sqlType);
	}
	
	/**
	 * Register a Java class to a SQL type mapping
	 *
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
	 * @param size the minimal size from which the SQL type will be used
	 */
	public void put(Class clazz, String sqlType, int size) {
		javaTypeToSqlType.put(clazz, sqlType, size);
	}
	
	/**
	 * Register a column to a SQL type mapping
	 *
	 * @param column the column to bind
	 * @param sqlType the SQL type to map on the Java type
	 */
	public void put(Column column, String sqlType) {
		columnToSQLType.put(column, sqlType);
	}
	
	/**
	 * Gives the SQL type name of a column. Main entry point of this class.
	 *
	 * @param column a column
	 * @return the SQL type for the given column
	 */
	public String getTypeName(Column column) {
		// first, very fine-grained tuning
		String typeName = columnToSQLType.get(column);
		if (typeName != null) {
			return typeName;
		}
		// then, tuning by Java type : same types may use same SQL type (id, timestamp, ...)
		Class javaType = column.getJavaType();
		Integer size = column.getSize();
		if (size != null) {
			return javaTypeToSqlType.getTypeName(javaType, size);
		} else {
			try {
				return javaTypeToSqlType.getTypeName(javaType);
			} catch (BindingException e) {
				// Exception is wrapped for a more accurate message (column in message)
				throw new BindingException("No sql type defined for column " + column.getAbsoluteName(), e);
			}
		}
	}
}
