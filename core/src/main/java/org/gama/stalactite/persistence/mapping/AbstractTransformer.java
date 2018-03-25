package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.gama.lang.Reflections;
import org.gama.sql.result.Row;

/**
 * A very general frame to transform {@link Row}s
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractTransformer<T> implements IRowTransformer<T> {
	
	protected final Constructor<T> constructor;
	
	public AbstractTransformer(Class<T> clazz) {
		this(Reflections.getDefaultConstructor(clazz));
	}
	
	public AbstractTransformer(Constructor<T> constructor) {
		this.constructor = constructor;
		Reflections.ensureAccessible(constructor);
	}
	
	@Override
	public T transform(Row row) {
		T bean = newBeanInstance();
		applyRowToBean(row, bean);
		return bean;
	}
	
	protected abstract void applyRowToBean(Row row, T bean);
	
	public T newBeanInstance() {
		T rowBean;
		try {
			rowBean = constructor.newInstance();
		} catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
			throw new RuntimeException("Class " + constructor.getDeclaringClass().getName() + " can't be instanciated", e);
		}
		return rowBean;
	}
}
