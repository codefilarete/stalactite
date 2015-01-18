package org.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.stalactite.lang.Reflections.FieldIterator;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.Filter;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

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
		Map<Field, Column> fieldToColumn = new LinkedHashMap<>(5);
		for (Field field : fields) {
			fieldToColumn.put(field, targetTable.new Column(getColumnName(field), Integer.class));
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
			return true;
		}
	}
}
