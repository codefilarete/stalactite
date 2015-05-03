package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import org.gama.lang.Reflections;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.sql.result.IRowTransformer;
import org.gama.stalactite.persistence.sql.result.Row;

/**
 * @author mary
 */
public abstract class ToCollectionRowTransformer<T extends Collection> implements IRowTransformer<T> {
	
	private final Constructor<T> constructor;
	
	public ToCollectionRowTransformer(Class<T> clazz) {
		this(Reflections.getDefaultConstructor(clazz));
	}
	
	protected ToCollectionRowTransformer(Constructor<T> constructor) {
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
