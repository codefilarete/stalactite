package org.codefilarete.stalactite.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * A very general frame to transform {@link Row}s
 *
 * @author Guillaume Mary
 */
public abstract class AbstractTransformer<C> implements RowTransformer<C> {
	
	protected final Function<ColumnedRow, C> beanFactory;
	
	private final Collection<TransformerListener<C>> rowTransformerListeners = new ArrayList<>();
	
	/**
	 * Constructor for beans to be instanced with their default constructor.
	 *
	 * @param clazz bean class
	 */
	public AbstractTransformer(Class<C> clazz) {
		this(row -> Reflections.newInstance(clazz));
	}
	
	/**
	 * Constructor with a general bean constructor
	 *
	 * @param beanFactory the factory of beans
	 */
	public AbstractTransformer(Function<? extends ColumnedRow, C> beanFactory) {
		this(beanFactory, Collections.emptyList());
	}
	
	protected AbstractTransformer(Function<? extends ColumnedRow, C> beanFactory,
								  Collection<TransformerListener<C>> rowTransformerListeners) {
		this.beanFactory = (Function<ColumnedRow, C>) beanFactory;
		this.rowTransformerListeners.addAll(rowTransformerListeners);
	}
	
	public Collection<TransformerListener<C>> getRowTransformerListeners() {
		return rowTransformerListeners;
	}
	
	@Override
	public void addTransformerListener(TransformerListener<? extends C> listener) {
		this.rowTransformerListeners.add((TransformerListener<C>) listener);
	}
	
	@Override
	public C transform(ColumnedRow row) {
		C bean = newBeanInstance(row);
		applyRowToBean(row, bean);
		this.rowTransformerListeners.forEach(listener -> listener.onTransform(bean, row::get));
		return bean;
	}
	
	/**
	 * Instantiates a bean 
	 * 
	 * @param row current {@link java.sql.ResultSet} row, may be used to defined which bean to instantiate
	 * @return a new instance of bean C
	 */
	@Override
	public C newBeanInstance(ColumnedRow row) {
		return beanFactory.apply(row::get);
	}
}
