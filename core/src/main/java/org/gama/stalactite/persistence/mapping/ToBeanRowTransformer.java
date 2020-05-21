package org.gama.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Class for transforming columns into a bean.
 * 
 * @author Guillaume Mary
 */
public class ToBeanRowTransformer<C> extends AbstractTransformer<C> {
	
	private final Map<Column, IMutator> columnToMember;
	
	private final Collection<TransformerListener<C>> rowTransformerListeners = new ArrayList<>();
	
	/**
	 * A constructor that maps all fields of a class by name
	 *
	 * @param clazz the instances class to be build
	 * @param table the mapped table
	 * @param warnOnMissingColumn indicates if an Exception must be thrown when no {@link Column} in the {@link Table} can be found for a field
	 * (matching name)
	 */
	public ToBeanRowTransformer(Class<C> clazz, Table table, boolean warnOnMissingColumn) {
		this(clazz, new HashMap<>(10));
		Map<String, Column> columnPerName = table.mapColumnsOnName();
		FieldIterator fieldIterator = new FieldIterator(clazz);
		Iterables.stream(fieldIterator).forEach(field -> {
					Column column = columnPerName.get(field.getName());
					if (column == null) {
						if (warnOnMissingColumn) {
							throw new UnsupportedOperationException("Missing column for field " + Reflections.toString(field));
						}
					} else {
						columnToMember.put(column, Accessors.mutatorByField(field));
					}
				}
		);
	}
	
	/**
	 *
	 * @param clazz the constructor to be used to instanciate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Class<C> clazz, Map<Column, IMutator> columnToMember) {
		super(clazz);
		this.columnToMember = columnToMember;
	}
	
	/**
	 *
	 * @param beanFactory factory to be used to instanciate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Function<Function<Column, Object>, C> beanFactory, Map<Column, IMutator> columnToMember) {
		super(beanFactory, new ColumnedRow());
		this.columnToMember = columnToMember;
	}
	
	protected ToBeanRowTransformer(Function<Function<Column, Object>, C> beanFactory, Map<Column, IMutator> columnToMember,
								   ColumnedRow columnedRow, Collection<TransformerListener<C>> rowTransformerListeners) {
		super(beanFactory, columnedRow);
		this.columnToMember = columnToMember;
		this.rowTransformerListeners.addAll(rowTransformerListeners);
	}
	
	public Map<Column, IMutator> getColumnToMember() {
		return columnToMember;
	}
	
	public Collection<TransformerListener<C>> getRowTransformerListeners() {
		return rowTransformerListeners;
	}
	
	/** Overriden to invoke tranformation listeners */
	@Override
	public C transform(Row row) {
		C bean = super.transform(row);
		this.rowTransformerListeners.forEach(listener -> listener.onTransform(bean, c-> getColumnedRow().getValue(c, row)));
		return bean;
	}
	
	@Override
	public void applyRowToBean(Row source, C targetRowBean) {
		for (Entry<Column, IMutator> columnFieldEntry : columnToMember.entrySet()) {
			Object propertyValue = getColumnedRow().getValue(columnFieldEntry.getKey(), source);
			applyValueToBean(targetRowBean, columnFieldEntry, propertyValue);
		}
	}
	
	protected void applyValueToBean(C targetRowBean, Entry<Column, IMutator> columnFieldEntry, Object propertyValue) {
		columnFieldEntry.getValue().set(targetRowBean, propertyValue);
	}
	
	/**
	 * Allows to change the mapping used by this instance to a new one. A new instance is returned that will read keys according to the given
	 * "sliding" function.
	 * Helpfull to reuse a {@link IRowTransformer} over multiple queries which different column aliases.
	 * 
	 * @param columnedRow a wrapper that gives {@link Row} values by {@link Column}.
	 * @return a new instance of {@link ToBeanRowTransformer} which read keys are those given by the function
	 */
	public ToBeanRowTransformer<C> copyWithAliases(ColumnedRow columnedRow) {
		return new ToBeanRowTransformer<>(beanFactory,
				new HashMap<>(this.columnToMember),
				columnedRow,
				// listeners are given to the new instance because they may be interested in transforming rows of this one
				rowTransformerListeners
		);
	}
	
	public void addTransformerListener(TransformerListener<C> listener) {
		this.rowTransformerListeners.add(listener);
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
