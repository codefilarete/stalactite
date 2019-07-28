package org.gama.stalactite.persistence.mapping;

import java.util.function.Supplier;

import org.gama.lang.Reflections;
import org.gama.stalactite.sql.result.Row;

/**
 * A very general frame to transform {@link Row}s
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractTransformer<T> implements IRowTransformer<T> {
	
	protected final Supplier<T> constructor;
	
	/**
	 * Constructor for beans to be instanciated with their default constructor.
	 * 
	 * @param clazz bean class
	 */
	public AbstractTransformer(Class<T> clazz) {
		this(() -> Reflections.newInstance(clazz));
	}
	
	/**
	 * Constructor with a general bean {@link Supplier}
	 *
	 * @param factory the factory of beans
	 */
	public AbstractTransformer(Supplier<T> factory) {
		this.constructor = factory;
	}
	
	@Override
	public T transform(Row row) {
		T bean = newBeanInstance();
		applyRowToBean(row, bean);
		return bean;
	}
	
	protected abstract void applyRowToBean(Row row, T bean);
	
	/**
	 * Instanciates a bean 
	 * 
	 * @return a new instance of bean T
	 */
	public T newBeanInstance() {
		return constructor.get();
	}
}
