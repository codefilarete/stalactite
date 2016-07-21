package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.Filter;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class PersistentFieldHarverster {
	
	private Map<Field, Column> fieldToColumn;
	
	private Map<String, Field> nameTofield;
	
	public List<Field> getFields(Class clazz) {
		FieldFilter fieldVisitor = getFieldVisitor();
		return Iterables.filter(new FieldIterator(clazz), fieldVisitor);
	}
	
	public Map<Field, Column> getFieldToColumn() {
		return fieldToColumn;
	}
	
	public Map<Field, Column> mapFields(Class clazz, Table targetTable) {
		List<Field> fields = getFields(clazz);
		Map<String, Column> mapColumnsOnName = targetTable.mapColumnsOnName();
		fieldToColumn = new LinkedHashMap<>(5);
		nameTofield = new HashMap<>(5);
		for (Field field : fields) {
			Column column = mapColumnsOnName.get(field.getName());
			if (column == null) {
				column = newColumn(targetTable, field);
			} // else column already exists, skip it to avoid duplicate column (even if type is different)
			fieldToColumn.put(field, column);
			nameTofield.put(field.getName(), field);
		}
		return fieldToColumn;
	}
	
	protected Column newColumn(Table targetTable, Field field) {
		return newColumn(targetTable, buildColumnName(field), field.getType());
	}
	
	protected Column newColumn(Table targetTable, String fieldName, Class type) {
		return targetTable.new Column(fieldName, type);
	}
	
	protected String buildColumnName(Field field) {
		return field.getName();
	}
	
	public Field getField(String name) {
		return nameTofield.get(name);
	}
	
	public Column getColumn(Field field) {
		return fieldToColumn.get(field);
	}
	
	/**
	 * To ovveride to exclude unecessary Fields. Implemented to exclude static, Iterable and Map fields.
	 * @return a {@link FieldFilter}
	 */
	protected FieldFilter getFieldVisitor() {
		return new FieldFilter();
	}
	
	/**
	 * Simple class that doesn't accept static, Iterable and Map Fields
	 */
	public static class FieldFilter extends Filter<Field> {
		@Override
		public boolean accept(Field field) {
			return !Modifier.isStatic(field.getModifiers()) && !(Iterable.class.isAssignableFrom(field.getType()) || Map.class.isAssignableFrom(field.getType()));
		}
	}
}
