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
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.persistence.structure.Table.SizedColumn;

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
	
	/**
	 * SQL types storage per Java type, usual cases.
	 */
	private final Map<Class, String> defaultJavaTypeToSQLType = new HashMap<>();
	
	/**
	 * Register a Java class to a SQL type mapping
	 * 
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
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
	 */
	public void put(Class clazz, int size, String sqlType) {
		javaTypeToSQLType.get(clazz).put(size, sqlType);
	}
	
	/**
	 * Same as {@link #put(Class, String)} but with fluent writing
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
	 * @return this
	 */
	public JavaTypeToSqlTypeMapping with(Class clazz, String sqlType) {
		put(clazz, sqlType);
		return this;
	}
	
	/**
	 * Same as {@link #put(Class, int, String)} but with fluent writing
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
	 * Gives the SQL type name of a column. Main entry point of this class.
	 *
	 * @param column a column
	 * @return the SQL type for the given column
	 */
	public String getTypeName(Column column) {
		Class javaType = column.getJavaType();
		if (javaType == null) {
			throw new IllegalArgumentException("Can't give sql type for column " + column.getAbsoluteName() + " because its type is null");
		}
		if (column instanceof SizedColumn) {
			int size = ((SizedColumn) column).getSize();
			return getTypeName(javaType, size);
		} else {
			return getTypeName(javaType);
		}
	}
	
	/**
	 * Gives the SQL type of a Java class
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
				// exception is replaced by a better message
				throw new IllegalArgumentException("No sql type defined for " + javaType);
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
				// NB: we use $l as Hibernate to ease an eventual switch between framworks
				typeName = typeName.replace("$l", String.valueOf(size));
			} else {
				typeName = getTypeName(javaType);
			}
			return typeName;
		}
	}
}
