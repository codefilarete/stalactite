package org.gama.stalactite.persistence.mapping;

import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.sql.result.Row;

/**
 * A very general frame to transform {@link Row}s
 *
 * @author Guillaume Mary
 */
public abstract class AbstractTransformer<T> implements IRowTransformer<T> {
	
	protected final Function<Function<Column, Object>, T> beanFactory;
	
	/** A kind of {@link Column} aliaser, mainly usefull in case of {@link #copyWithAliases(ColumnedRow)} usage */
	private final ColumnedRow columnedRow;
	
	/**
	 * Constructor for beans to be instanciated with their default constructor.
	 *
	 * @param clazz bean class
	 */
	public AbstractTransformer(Class<T> clazz) {
		this(row -> Reflections.newInstance(clazz), new ColumnedRow());
	}
	
	/**
	 * Constructor with a general bean constructor
	 *
	 * @param factory the factory of beans
	 */
	public AbstractTransformer(Function<Function<Column, Object>, T> factory, ColumnedRow columnedRow) {
		this.beanFactory = factory;
		this.columnedRow = columnedRow;
	}
	
	public ColumnedRow getColumnedRow() {
		return columnedRow;
	}
	
	public abstract AbstractTransformer<T> copyWithAliases(ColumnedRow columnedRow);
	
	@Override
	public T transform(Row row) {
		T bean = newBeanInstance(row);
		applyRowToBean(row, bean);
		return bean;
	}
	
	protected abstract void applyRowToBean(Row row, T bean);
	
	/**
	 * Instanciates a bean 
	 * 
	 * @param row current {@link java.sql.ResultSet} row, may be used to defined which bean to instanciate
	 * @return a new instance of bean T
	 */
	public T newBeanInstance(Row row) {
		return (T) beanFactory.apply(c -> this.columnedRow.getValue(c, row));
	}
}
