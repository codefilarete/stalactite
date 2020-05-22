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
	
	public abstract AbstractTransformer<C> copyWithAliases(ColumnedRow columnedRow);
	
	@Override
	public C transform(Row row) {
		C bean = newBeanInstance(row);
		applyRowToBean(row, bean);
		this.rowTransformerListeners.forEach(listener -> listener.onTransform(bean, c-> getColumnedRow().getValue(c, row)));
		return bean;
	}
	
	public abstract void applyRowToBean(Row row, C bean);
	
	/**
	 * Instanciates a bean 
	 * 
	 * @param row current {@link java.sql.ResultSet} row, may be used to defined which bean to instanciate
	 * @return a new instance of bean C
	 */
	public C newBeanInstance(Row row) {
		return (C) beanFactory.apply(c -> this.columnedRow.getValue(c, row));
	}
	
	
	/**
	 * Small interface which instances will be invoked after row transformation, such as one can add any post-treatment to the bean row
	 * @param <C> the row bean
	 */
	@FunctionalInterface
	public interface TransformerListener<C> {
		
		/**
		 * Method invoked for each read row after all transformations made by a {@link ToBeanRowTransformer} on a bean, so the bean is considered
		 * "complete".
		 *
		 * @param c current row bean, may be dfferent from row to row depending on bean instanciation policy of bean factory given
		 * 		to {@link ToBeanRowTransformer} at construction time 
		 * @param rowValueProvider a function that let one read a value from current row without exposing internal mecanism of row reading.
		 *  Input is a {@link Column} because it is safer than a simple column name because {@link ToBeanRowTransformer} can be copied with
		 *  different aliases making mistach when value is read from name.
		 */
		void onTransform(C c, Function<Column, Object> rowValueProvider);
		
	}
}
