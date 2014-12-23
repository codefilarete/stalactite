package org.stalactite.persistence.sql.ddl;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;

import org.stalactite.lang.collection.EntryFactoryHashMap;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.persistence.structure.Table.SizedColumn;

/**
 * @author mary
 */
public class JavaTypeToSqlTypeMapping {
	
	private Map<Class, SortedMap<Integer, String>> javaTypeToSQLType = new EntryFactoryHashMap<Class, SortedMap<Integer, String>>() {
		@Override
		public TreeMap<Integer, String> createInstance(Class input) {
			return new TreeMap<>();
		}
	};
	private Map<Class, String> defaultJavaTypeToSQLType = new HashMap<>();
	
	public void put(@Nonnull Class clazz, @Nonnull String sqlType) {
		defaultJavaTypeToSQLType.put(clazz, sqlType);
	}
	
	public void put(@Nonnull Class clazz, int size, @Nonnull String sqlType) {
		javaTypeToSQLType.get(clazz).put(size, sqlType);
	}
	
	public String getTypeName(Column column) {
		if (column instanceof SizedColumn) {
			int size = ((SizedColumn) column).getSize();
			return getTypeName(column.getJavaType(), size);
		} else {
			return getTypeName(column.getJavaType());
		}
	}
	
	/**
	 * Renvoie le type SQL par défaut pour une classe Java
	 * @param javaType
	 * @return
	 */
	public String getTypeName(Class javaType) {
		return defaultJavaTypeToSQLType.get(javaType);
	}
	
	/**
	 * Renvoie le type SQL le plus proche pour une classe Java
	 * @param javaType
	 * @param size
	 * @return
	 */
	public String getTypeName(Class javaType, Integer size) {
		if (size == null) {
			return getTypeName(javaType);
		} else {
			SortedMap<Integer, String> typeNames = javaTypeToSQLType.get(javaType).tailMap(size);
			String typeName = Iterables.firstValue(typeNames);
			if (typeName != null) {
				// NB: on utilise $l comme Hibernate pour simplifier une éventuelle transition
				typeName = typeName.replace("$l", String.valueOf(size));
			} else {
				typeName = getTypeName(javaType);
			}
			return typeName;
		}
	}
}
