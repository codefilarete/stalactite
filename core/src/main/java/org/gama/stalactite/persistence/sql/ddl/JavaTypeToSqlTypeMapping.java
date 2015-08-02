package org.gama.stalactite.persistence.sql.ddl;

import org.gama.lang.bean.IFactory;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.persistence.structure.Table.SizedColumn;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Mapping between Java classes and Sql Types.
 * Near Hibernate Dialect::register types principles.
 * 
 * @author mary
 */
public class JavaTypeToSqlTypeMapping {
	
	private final Map<Class, SortedMap<Integer, String>> javaTypeToSQLType = new ValueFactoryHashMap<>(new IFactory<Class, SortedMap<Integer, String>>() {
		@Override
		public SortedMap<Integer, String> createInstance(Class input) {
			return new TreeMap<>();
		}
	});
	private final Map<Class, String> defaultJavaTypeToSQLType = new HashMap<>();
	
	public void put(@Nonnull Class clazz, @Nonnull String sqlType) {
		defaultJavaTypeToSQLType.put(clazz, sqlType);
	}
	
	public void put(@Nonnull Class clazz, int size, @Nonnull String sqlType) {
		javaTypeToSQLType.get(clazz).put(size, sqlType);
	}
	
	public String getTypeName(Column column) {
		Class javaType = column.getJavaType();
		if (javaType == null) {
			throw new IllegalArgumentException("Can't give sql type for column "+column.getAbsoluteName()+" because its type is null");
		}
		if (column instanceof SizedColumn) {
			int size = ((SizedColumn) column).getSize();
			return getTypeName(javaType, size);
		} else {
			return getTypeName(javaType);
		}
	}
	
	/**
	 * Renvoie le type SQL par défaut pour une classe Java
	 * @param javaType
	 * @return
	 */
	public String getTypeName(Class javaType) {
		String type = defaultJavaTypeToSQLType.get(javaType);
		if (type == null) {
			throw new IllegalArgumentException("No sql type defined for "+javaType+" in dialect "+ PersistenceContext.getCurrent().getDialect().getClass().getName());
		}
		return type;
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
