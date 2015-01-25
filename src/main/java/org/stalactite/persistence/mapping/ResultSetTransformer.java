package org.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.stalactite.lang.Reflections.FieldIterator;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.reflection.AccessorByField;

/**
 * @author mary
 */
public class ResultSetTransformer<E> implements IResultSetTransformer<E> {
	
	private final Constructor<E> constructor;
	private final Map<String, AccessorByField> columnToField;
	
	public ResultSetTransformer(Class<E> clazz) throws NoSuchMethodException {
		this.constructor = clazz.getConstructor();
		this.constructor.setAccessible(true);
		this.columnToField = new HashMap<>(10);
		FieldIterator fieldIterator = new FieldIterator(clazz);
		Iterables.visit(fieldIterator, new ForEach<Field, Void>() {
			@Override
			public Void visit(Field field) {
				columnToField.put(field.getName(), new AccessorByField(field));
				return null;
			}
		});
	}
	
	public ResultSetTransformer(Constructor<E> constructor, Map<String, AccessorByField> columnToField) {
		this.constructor = constructor;
		this.constructor.setAccessible(true);
		this.columnToField = columnToField;
	}
	
	@Override
	public E transform(ResultSet resultSet) {
		new ResultSetIterator(resultSet).next();
		E bean = newRowInstance();
		convertColumnsToProperties(resultSet, bean);
		return bean;
	}
	
	protected E newRowInstance() {
		E rowBean = null;
		try {
			rowBean = constructor.newInstance();
		} catch (IllegalAccessException|InstantiationException|InvocationTargetException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return rowBean;
	}
	
	@Override
	public void convertColumnsToProperties(ResultSet resultSet, E rowBean) {
		try {
			for (Entry<String, AccessorByField> columnFieldEntry : columnToField.entrySet()) {
				Object object = resultSet.getObject(columnFieldEntry.getKey());
				columnFieldEntry.getValue().set(rowBean, object);
			}
		} catch(IllegalAccessException | SQLException e){
			Exceptions.throwAsRuntimeException(e);
		}
	}
}
