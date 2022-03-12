package org.codefilarete.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.tool.bean.FieldIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public class PersistentFieldHarverster {
	
	private Map<PropertyAccessor, Column> fieldToColumn;
	
	private Map<String, Field> nameTofield;
	
	public List<Field> getFields(Class clazz) {
		FieldFilter fieldVisitor = getFieldVisitor();
		return Iterables.stream(new FieldIterator(clazz)).filter(fieldVisitor).collect(Collectors.toList());
	}
	
	public Map<PropertyAccessor, Column> getFieldToColumn() {
		return fieldToColumn;
	}
	
	public <C, T extends Table> Map<PropertyAccessor<C, Object>, Column<T, Object>> mapFields(Class<C> clazz, T targetTable) {
		List<Field> fields = getFields(clazz);
		Map<String, Column<T, Object>> mapColumnsOnName = targetTable.mapColumnsOnName();
		fieldToColumn = new LinkedHashMap<>(5);
		nameTofield = new HashMap<>(5);
		for (Field field : fields) {
			Column column = mapColumnsOnName.get(field.getName());
			if (column == null) {
				column = newColumn(targetTable, field);
			} // else column already exists, skip it to avoid duplicate column (even if type is different)
			fieldToColumn.put(Accessors.propertyAccessor(field), column);
			nameTofield.put(field.getName(), field);
		}
		return (Map) fieldToColumn;
	}
	
	protected <T extends Table> Column<T, Object> newColumn(T targetTable, Field field) {
		return newColumn(targetTable, buildColumnName(field), field.getType());
	}
	
	protected <T extends Table> Column<T, Object> newColumn(T targetTable, String fieldName, Class type) {
		return targetTable.addColumn(fieldName, type);
	}
	
	protected String buildColumnName(Field field) {
		return field.getName();
	}
	
	public Field getField(String name) {
		return nameTofield.get(name);
	}
	
	public Column getColumn(PropertyAccessor field) {
		return fieldToColumn.get(field);
	}
	
	/**
	 * To be overriden to exclude unecessary Fields. Current implementation excludes static, Iterable and Map fields.
	 * @return a {@link FieldFilter}
	 */
	protected FieldFilter getFieldVisitor() {
		return new FieldFilter();
	}
	
	/**
	 * Simple class that doesn't accept static fields
	 */
	public static class FieldFilter implements Predicate<Field> {
		@Override
		public boolean test(Field field) {
			return !Modifier.isStatic(field.getModifiers());
		}
	}
}
