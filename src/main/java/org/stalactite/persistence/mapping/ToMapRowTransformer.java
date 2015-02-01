package org.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.stalactite.lang.Reflections;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.sql.result.IRowTransformer;
import org.stalactite.persistence.sql.result.Row;

/**
 * @author mary
 */
public abstract class ToMapRowTransformer<T extends Map> implements IRowTransformer<T> {
	
	private final Constructor<T> constructor;
	
	public ToMapRowTransformer(Constructor<T> constructor) {
		this.constructor = constructor;
		Reflections.ensureAccessible(constructor);
	}
	
	@Override
	public T transform(Row row) {
		T bean = newRowInstance();
		convertRowContentToMap(row, bean);
		return bean;
	}
	
	protected abstract void convertRowContentToMap(Row row, T bean);
	
	protected T newRowInstance() {
		T rowBean = null;
		try {
			rowBean = constructor.newInstance();
		} catch (IllegalAccessException|InstantiationException|InvocationTargetException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return rowBean;
	}
}
