package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.result.Row;

/**
 * Class for transforming columns into a bean.
 * 
 * @author Guillaume Mary
 */
public class ToBeanRowTransformer<T> extends AbstractTransformer<T> {
	
	private final Map<String, PropertyAccessor> columnToField;
	
	public ToBeanRowTransformer(Class<T> clazz) throws NoSuchMethodException {
		this(clazz.getConstructor(), new HashMap<>(10));
		FieldIterator fieldIterator = new FieldIterator(clazz);
		Iterables.visit(fieldIterator, new ForEach<Field, Void>() {
			@Override
			public Void visit(Field field) {
				columnToField.put(field.getName(), PropertyAccessor.forProperty(field.getDeclaringClass(), field.getName()));
				return null;
			}
		});
	}
	
	public ToBeanRowTransformer(Constructor<T> constructor, Map<String, PropertyAccessor> columnToField) {
		super(constructor);
		this.columnToField = columnToField;
	}
	
	@Override
	public void applyRowToBean(Row row, T rowBean) {
		for (Entry<String, PropertyAccessor> columnFieldEntry : columnToField.entrySet()) {
			Object object = row.get(columnFieldEntry.getKey());
			columnFieldEntry.getValue().set(rowBean, object);
		}
	}
}
