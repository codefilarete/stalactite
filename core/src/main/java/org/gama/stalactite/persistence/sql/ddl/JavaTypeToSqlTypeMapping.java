package org.gama.stalactite.persistence.sql.ddl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.gama.lang.bean.InterfaceIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.sql.dml.SQLStatement.BindingException;
import org.gama.stalactite.persistence.structure.Column;

/**
 * A storage of mapping between Java classes and Sql Types. Aimed at generating schema, not reading nor writing to ResultSet/Statement.
 * Near Hibernate Dialect::register types principles.
 *
 * @author Guillaume Mary
 * @see #getTypeName(Class)
 */
public class JavaTypeToSqlTypeMapping {
	
	/**
	 * SQL types storage per Java type, dedicated to sized-types.
	 * Values are SortedMaps of size to SQL type. SortedMap are used to ease finding of types per size
	 */
	private final Map<Class, SortedMap<Integer, String>> javaTypeToSQLType = new ValueFactoryHashMap<>(input -> new TreeMap<>());
	
	private final Map<Column, String> columnToSQLType = new HashMap<>();
	
	/**
	 * SQL types storage per Java type, usual cases.
	 */
	private final Map<Class, String> defaultJavaTypeToSQLType = new HashMap<>();
	
	/**
	 * Registers a Java class to a SQL type mapping
	 * 
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
	 * @see #with(Class, String)
	 */
	public void put(Class clazz, String sqlType) {
		defaultJavaTypeToSQLType.put(clazz, sqlType);
	}
	
	/**
	 * Register a Java class to a SQL type mapping
	 *
	 * @param clazz the Java class to bind
	 * @param size the minimal size from which the SQL type will be used
	 * @param sqlType the SQL type to map on the Java type
	 * @see #with(Class, int, String)
	 */
	public void put(Class clazz, int size, String sqlType) {
		javaTypeToSQLType.get(clazz).put(size, sqlType);
	}
	
	/**
	 * Register a column to a SQL type mapping
	 *
	 * @param column the column to bind
	 * @param sqlType the SQL type to map on the Java type
	 * @see #with(Column, String) 
	 */
	public void put(Column column, String sqlType) {
		columnToSQLType.put(column, sqlType);
	}
	
	/**
	 * Same as {@link #put(Class, String)} with fluent writing
	 * 
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
	 * @return this
	 */
	public JavaTypeToSqlTypeMapping with(Class clazz, String sqlType) {
		put(clazz, sqlType);
		return this;
	}
	
	/**
	 * Same as {@link #put(Class, int, String)} with fluent writing
	 * 
	 * @param clazz the Java class to bind
	 * @param size the minimal size from which the SQL type will be used
	 * @param sqlType the SQL type to map on the Java type
	 * @return this
	 */
	public JavaTypeToSqlTypeMapping with(Class clazz, int size, String sqlType) {
		put(clazz, size, sqlType);
		return this;
	}
	
	/**
	 * Same as {@link #put(Column, String)} with fluent writing
	 *
	 * @param column the column to bind
	 * @param sqlType the SQL type to map on the Java type
	 */
	public JavaTypeToSqlTypeMapping with(Column column, String sqlType) {
		put(column, sqlType);
		return this;
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
			return getTypeName(javaType, size);
		} else {
			try {
				return getTypeName(javaType);
			} catch (BindingException e) {
				// Exception is wrapped for a more accurate message (column in message)
				throw new BindingException("No sql type defined for column " + column.getAbsoluteName(), e);
			}
		}
	}
	
	/**
	 * Gives the SQL type of a Java class, checks also for its interfaces 
	 *
	 * @param javaType a Java class
	 * @return the SQL type for the given column
	 */
	private String getTypeName(Class javaType) {
		String type = defaultJavaTypeToSQLType.get(javaType);
		if (type == null) {
			InterfaceIterator interfaceIterator = new InterfaceIterator(javaType);
			Stream<String> stream = Iterables.stream(interfaceIterator).map(defaultJavaTypeToSQLType::get);
			type = stream.filter(Objects::nonNull).findFirst().orElse(null);
			if (type != null) {
				return type;
			} else {
				throw new BindingException("No sql type defined for " + javaType);
			}
		}
		return type;
	}
	
	/**
	 * Gives the nearest SQL type of a Java class according to the expected size
	 *
	 * @param javaType a Java class
	 * @return the SQL type for the given column
	 */
	String getTypeName(Class javaType, Integer size) {
		if (size == null) {
			return getTypeName(javaType);
		} else {
			SortedMap<Integer, String> typeNames = javaTypeToSQLType.get(javaType).tailMap(size);
			String typeName = Iterables.firstValue(typeNames);
			if (typeName != null) {
				// NB: we use $l as Hibernate to ease an eventual switch between frameworks
				typeName = typeName.replace("$l", String.valueOf(size));
			} else {
				typeName = getTypeName(javaType);
			}
			return typeName;
		}
	}
}
