package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.gama.lang.Reflections.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.Filter;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class PersistentFieldHarverster {
	
	public List<Field> getFields(Class clazz) {
		FieldFilter fieldVisitor = getFieldVisitor();
		return Iterables.filter(new FieldIterator(clazz), fieldVisitor);
	}
	
	public Map<Field, Column> mapFields(Class clazz, Table targetTable) {
		List<Field> fields = getFields(clazz);
		Map<String, Column> mapColumnsOnName = targetTable.mapColumnsOnName();
		Map<Field, Column> fieldToColumn = new LinkedHashMap<>(5);
		for (Field field : fields) {
			Column column = mapColumnsOnName.get(field.getName());
			if (column == null) {
				column = targetTable.new Column(getColumnName(field), field.getType());
			}
			fieldToColumn.put(field, column);
		}
		return fieldToColumn;
	}
	
	protected String getColumnName(Field field) {
		return field.getName();
	}
	
	/**
	 * Méthode à surcharger/modifier pour prendre en compte les Map par exemple
	 * @return
	 */
	@Nonnull
	protected FieldFilter getFieldVisitor() {
		return new FieldFilter();
	}
	
	public static class FieldFilter extends Filter<Field> {
		@Override
		public boolean accept(Field field) {
			return !Modifier.isStatic(field.getModifiers()) && !(Iterable.class.isAssignableFrom(field.getType()) || Map.class.isAssignableFrom(field.getType()));
		}
	}
}
