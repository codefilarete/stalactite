package org.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.stalactite.lang.Reflections.FieldIterator;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.sql.result.IRowTransformer;
import org.stalactite.persistence.sql.result.Row;
import org.stalactite.reflection.PropertyAccessor;

/**
 * @author mary
 */
public class ToBeanRowTransformer<T> implements IRowTransformer<T> {
	
	private final Constructor<T> constructor;
	private final Map<String, PropertyAccessor> columnToField;
	
	public ToBeanRowTransformer(Class<T> clazz) throws NoSuchMethodException {
		this.constructor = clazz.getConstructor();
		this.constructor.setAccessible(true);
		this.columnToField = new HashMap<>(10);
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
		this.constructor = constructor;
		this.constructor.setAccessible(true);
		this.columnToField = columnToField;
	}
	
	@Override
	public T transform(Row row) {
		T bean = newRowInstance();
		convertColumnsToProperties(row, bean);
		return bean;
	}
	
	protected T newRowInstance() {
		T rowBean = null;
		try {
			rowBean = constructor.newInstance();
		} catch (IllegalAccessException|InstantiationException|InvocationTargetException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return rowBean;
	}
	
	public void convertColumnsToProperties(Row row, T rowBean) {
		try {
			for (Entry<String, PropertyAccessor> columnFieldEntry : columnToField.entrySet()) {
				Object object = row.get(columnFieldEntry.getKey());
				columnFieldEntry.getValue().set(rowBean, object);
			}
		} catch(IllegalAccessException e){
			Exceptions.throwAsRuntimeException(e);
		}
	}
}
