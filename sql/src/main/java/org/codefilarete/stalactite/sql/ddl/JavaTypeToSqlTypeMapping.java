package org.codefilarete.stalactite.sql.ddl;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.codefilarete.tool.bean.InterfaceIterator;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * A storage for mapping between Java classes and Sql Types. Aimed at generating schema, not reading nor writing to
 * ResultSet/Statement.
 * A default registry is implemented through {@link DefaultTypeMapping}.
 *
 * @author Guillaume Mary
 * @see #getTypeName(Class)
 */
public class JavaTypeToSqlTypeMapping {
	
	/**
	 * SQL types storage per Java type, dedicated to sized-types.
	 * Values are {@link SortedMap}s of size to SQL type. {@link SortedMap} are used to ease finding of types per size
	 */
	private final Map<Class, SQLTypeHolder> javaTypeToSQLType = new HashMap<>(50);
	
	/**
	 * Registers a Java class to a SQL type mapping
	 * 
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
	 * @see #with(Class, String)
	 */
	public void put(Class clazz, String sqlType) {
		put(clazz, sqlType, null);
	}
	
	/**
	 * Register a Java class to a SQL type mapping
	 *
	 * @param clazz the Java class to bind
	 * @param sqlType the SQL type to map on the Java type
	 * @param size the maximum size until which the SQL type will be used
	 * @see #with(Class, Size, String)
	 */
	public void put(Class clazz, String sqlType, Size size) {
		javaTypeToSQLType.computeIfAbsent(clazz, k -> new SQLTypeHolder()).setType(sqlType, size);
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
	 * Same as {@link #put(Class, String, Size)} with fluent writing
	 * 
	 * @param clazz the Java class to bind
	 * @param size the minimal size from which the SQL type will be used
	 * @param sqlType the SQL type to map on the Java type
	 * @return this
	 */
	public JavaTypeToSqlTypeMapping with(Class clazz, Size size, String sqlType) {
		put(clazz, sqlType, size);
		return this;
	}
	
	public void replace(Class clazz, String sqlType) {
		replace(clazz, sqlType, null);
	}
	
	public void replace(Class clazz, String sqlType, Size size) {
		SQLTypeHolder replacingValue = new SQLTypeHolder();
		replacingValue.setType(sqlType, size);
		javaTypeToSQLType.put(clazz, replacingValue);
	}
	
	/**
	 * Gives the SQL type of a Java class, checks also for its interfaces
	 *
	 * @param javaType a Java class
	 * @return the SQL type for the given column or null if not found
	 */
	public String getTypeName(Class javaType) {
		return getTypeName(javaType, null);
	}
	
	/**
	 * Gives the nearest SQL type of a Java class according to the expected size
	 *
	 * @param javaType a Java class
	 * @return the SQL type for the given column
	 */
	public String getTypeName(Class javaType, @Nullable Size size) {
		SQLTypeHolder type = javaTypeToSQLType.get(javaType);
		if (type == null) {
			if (javaType.isEnum()) {
				type = javaTypeToSQLType.get(Enum.class);
			} else {
				InterfaceIterator interfaceIterator = new InterfaceIterator(javaType);
				Stream<SQLTypeHolder> stream = Iterables.stream(interfaceIterator).map(javaTypeToSQLType::get);
				type = stream.filter(Objects::nonNull).findFirst().orElse(null);
			}
		}
		return nullable(type).map(dataType -> dataType.getType(size)).getOr((String) null);
	}
	
	private static class SQLTypeHolder {
		
		private final SortedMap<Size, String> availableSizes = new TreeMap<>(Comparator.nullsLast((size1, size2) -> {
			if (size1 == size2) {
				return 0;
			}
			if (size1 == null) {
				return -1;
			}
			if (size2 == null) {
				return 1;
			}
			
			if (size1 instanceof Length && size2 instanceof Length) {
				return Integer.compare(((Length) size1).getValue(), ((Length) size2).getValue());
			} else if (size1 instanceof FixedPoint && size2 instanceof FixedPoint) {
				// return -1 to make new one replace the existing one. Hack.
				// because there's no reason to compare FixedPoint between them as for Length since "decimal(7, 3)" is not lower than "decimal(10)"
				return -1;
			} else {
				// If comparing different types, you might want to define a consistent ordering
				return size1.getClass().getName().compareTo(size2.getClass().getName());
			}
		}));
		
		public String getType(@Nullable Size size) {
			if (size == null) {
				return getTypeForLength(null);
			} else {
				return getTypeForLength(size);
			}
		}
		
		public void setType(String sqlTypeName, @Nullable Size size) {
			availableSizes.put(size, sqlTypeName);
		}
		
		private String getTypeForLength(@Nullable Size size) {
			String typeName;
			if (size == null && availableSizes.size() == 1) {
				Entry<Size, String> lonelyEntry = Iterables.first(availableSizes);
				typeName = lonelyEntry.getValue();
				size = lonelyEntry.getKey();
			} else {
				SortedMap<Size, String> typeNames = availableSizes.tailMap(size);
				typeName = Iterables.firstValue(typeNames);
			}
			if (typeName != null) {
				if (size instanceof Length) {
					// NB: we use $l as Hibernate to ease an eventual switch between frameworks
					typeName = typeName.replace("$l", String.valueOf(((Length) size).getValue()));
				} else if (size instanceof FixedPoint) {
					typeName = typeName.replace("$p", String.valueOf(((FixedPoint) size).getPrecision()));
					typeName = typeName.replace("$s", String.valueOf(((FixedPoint) size).getScale()));
				}
			}
			return typeName;
		}
	}
}
