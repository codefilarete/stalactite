package org.gama.stalactite.persistence.mapping;

import org.gama.lang.Reflections;
import org.gama.stalactite.persistence.sql.result.IRowTransformer;
import org.gama.stalactite.persistence.sql.result.Row;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * 
 * @author Guillaume Mary
 */
public abstract  class AbstractTransformer<T> implements IRowTransformer<T> {
	protected final Constructor<T> constructor;
	
	public AbstractTransformer(Constructor<T> constructor) {
		this.constructor = constructor;
		Reflections.ensureAccessible(constructor);
	}
	
	@Override
	public T transform(Row row) {
		T bean = newRowInstance();
		applyRowToBean(row, bean);
		return bean;
	}
	
	protected abstract void applyRowToBean(Row row, T bean);
	
	protected T newRowInstance() {
		T rowBean;
		try {
			rowBean = constructor.newInstance();
		} catch (IllegalAccessException|InstantiationException|InvocationTargetException e) {
			throw new RuntimeException("Class " + constructor.getDeclaringClass().getName()
					+ " can't be instanciated", e);
		}
		return rowBean;
	}
}
