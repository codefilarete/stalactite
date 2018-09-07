package org.gama.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Class for transforming columns into a bean.
 * 
 * @author Guillaume Mary
 */
public class ToBeanRowTransformer<T> extends AbstractTransformer<T> {
	
	private final Map<Column, IMutator> columnToMember;
	
	private final Collection<TransformerListener<T>> rowTransformerListeners = new ArrayList<>();
	
	/** A kind of {@link Column} aliaser, mainly usefull in case of {@link #withAliases(Function)} usage */
	private ColumnedRow columnedRow = new ColumnedRow();
	
	/**
	 * A constructor that maps all fields of a class by name
	 *
	 * @param clazz the instances class to be build
	 * @param table the mapped table
	 * @param warnOnMissingColumn indicates if an Exception must be thrown when no {@link Column} in the {@link Table} can be found for a field
	 * (matching name)
	 */
	public ToBeanRowTransformer(Class<T> clazz, Table table, boolean warnOnMissingColumn) {
		this(clazz, new HashMap<>(10), true);
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
	public ToBeanRowTransformer(Class<T> clazz, Map<Column, IMutator> columnToMember) {
		this(clazz, columnToMember, true);
	}
	
	/**
	 * 
	 * @param clazz the constructor to be used to instanciate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 * @param constructorProof a mark to distinguish this constructor from {@link #ToBeanRowTransformer(Class, Map)} due to type erasure
	 */
	private ToBeanRowTransformer(Class<T> clazz, Map<Column, IMutator> columnToMember, boolean constructorProof) {
		super(clazz);
		this.columnToMember = columnToMember;
	}
	
	/**
	 * 
	 * @param supplier the constructor to be used to instanciate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 */
	private ToBeanRowTransformer(Supplier<T> supplier, Map<Column, IMutator> columnToMember) {
		super(supplier);
		this.columnToMember = columnToMember;
	}
	
	protected ColumnedRow getColumnedRow() {
		return columnedRow;
	}
	
	/** Overriden to invoke tranformation listeners */
	@Override
	public T transform(Row row) {
		T bean = super.transform(row);
		this.rowTransformerListeners.forEach(listener -> listener.onTransform(bean, row));
		return bean;
	}
	
	@Override
	public void applyRowToBean(Row source, T targetRowBean) {
		for (Entry<Column, IMutator> columnFieldEntry : columnToMember.entrySet()) {
			Object propertyValue = columnedRow.getValue(columnFieldEntry.getKey(), source);
			columnFieldEntry.getValue().set(targetRowBean, propertyValue);
		}
	}
	
	/**
	 * Allows to change the mapping used by this instance to a new one. A new instance is returned that will read keys according to the given
	 * "sliding" function.
	 * Helpfull to reuse a {@link IRowTransformer} over multiple queries which different column aliases.
	 * 
	 * @param aliasProvider a function that gives new {@link Row} keys from some {@link Column}.
	 * @return a new instance of {@link ToBeanRowTransformer} whose read keys are those given by the function
	 */
	public ToBeanRowTransformer<T> withAliases(Function<Column, String> aliasProvider) {
		ToBeanRowTransformer<T> result = new ToBeanRowTransformer<>(constructor, new HashMap<>(this.columnToMember));
		result.columnedRow = new ColumnedRow(aliasProvider);
		// listeners are given to the new instance because they may be interested in transforming rows of this one
		result.rowTransformerListeners.addAll(rowTransformerListeners);
		return result;
	}
	
	
	public void addTransformerListener(TransformerListener<T> listener) {
		this.rowTransformerListeners.add(listener);
	}
	
	public interface TransformerListener<C> {
		
		void onTransform(C c, Row row);
		
	}
}
