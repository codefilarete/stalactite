package org.gama.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.sql.result.Row;

/**
 * A very general frame to transform {@link Row}s
 *
 * @author Guillaume Mary
 */
public abstract class AbstractTransformer<C> implements IRowTransformer<C> {
	
	protected final Function<Function<Column, Object>, C> beanFactory;
	
	/** A kind of {@link Column} aliaser, mainly usefull in case of {@link #copyWithAliases(ColumnedRow)} usage */
	private final ColumnedRow columnedRow;
	
	private final Collection<TransformerListener<C>> rowTransformerListeners = new ArrayList<>();
	
	/**
	 * Constructor for beans to be instanciated with their default constructor.
	 *
	 * @param clazz bean class
	 */
	public AbstractTransformer(Class<C> clazz) {
		this(row -> Reflections.newInstance(clazz), new ColumnedRow());
	}
	
	/**
	 * Constructor with a general bean constructor
	 *
	 * @param beanFactory the factory of beans
	 */
	public AbstractTransformer(Function<Function<Column, Object>, C> beanFactory, ColumnedRow columnedRow) {
		this(beanFactory, columnedRow, Collections.emptyList());
	}
	
	protected AbstractTransformer(Function<Function<Column, Object>, C> beanFactory, ColumnedRow columnedRow,
								  Collection<TransformerListener<C>> rowTransformerListeners) {
		this.beanFactory = beanFactory;
		this.columnedRow = columnedRow;
		this.rowTransformerListeners.addAll(rowTransformerListeners);
	}
	
	public ColumnedRow getColumnedRow() {
		return columnedRow;
	}
	
	public Collection<TransformerListener<C>> getRowTransformerListeners() {
		return rowTransformerListeners;
	}
	
	public void addTransformerListener(TransformerListener<C> listener) {
		this.rowTransformerListeners.add(listener);
	}
	
	@Override
	public C transform(Row row) {
		C bean = newBeanInstance(row);
		applyRowToBean(row, bean);
		this.rowTransformerListeners.forEach(listener -> listener.onTransform(bean, c-> getColumnedRow().getValue(c, row)));
		return bean;
	}
	
	/**
	 * Instanciates a bean 
	 * 
	 * @param row current {@link java.sql.ResultSet} row, may be used to defined which bean to instanciate
	 * @return a new instance of bean C
	 */
	protected C newBeanInstance(Row row) {
		return (C) beanFactory.apply(c -> this.columnedRow.getValue(c, row));
	}
}
