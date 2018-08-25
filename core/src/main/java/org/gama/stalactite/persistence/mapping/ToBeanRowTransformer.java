package org.gama.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

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
	
	private final Map<RowKeySupport, IMutator> keyToField;
	
	private final Collection<TransformerListener<T>> rowTransformerListeners = new ArrayList<>();
	
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
						throw new UnsupportedOperationException("Missing column for field " + field.getDeclaringClass().getName() + "." + field.getName());
					}
				} else {
					keyToField.put(new RowKeySupport(column), Accessors.mutator(field.getDeclaringClass(), field.getName()));
				}
			}
		);
	}
	
	/**
	 *
	 * @param clazz the constructor to be used to instanciate a new bean
	 * @param columnToField the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Class<T> clazz, Map<Column, IMutator> columnToField) {
		this(clazz, new HashMap<>(10), true);
		columnToField.forEach((key, value) -> keyToField.put(new RowKeySupport(key), value));
	}
	
	/**
	 * 
	 * @param clazz the constructor to be used to instanciate a new bean
	 * @param keyToField the mapping between key in rows and helper that fixes values of the bean
	 * @param constructorProof a mark to distinguish this constructor from {@link #ToBeanRowTransformer(Class, Map)} due to type erasure
	 */
	private ToBeanRowTransformer(Class<T> clazz, Map<RowKeySupport, IMutator> keyToField, boolean constructorProof) {
		super(clazz);
		this.keyToField = keyToField;
	}
	
	/**
	 * 
	 * @param supplier the constructor to be used to instanciate a new bean
	 * @param keyToField the mapping between key in rows and helper that fixes values of the bean
	 */
	private ToBeanRowTransformer(Supplier<T> supplier, Map<RowKeySupport, IMutator> keyToField) {
		super(supplier);
		this.keyToField = keyToField;
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
		for (Entry<RowKeySupport, IMutator> columnFieldEntry : keyToField.entrySet()) {
			String columnName = columnFieldEntry.getKey().rowKey();
			Object object = source.get(columnName);
			columnFieldEntry.getValue().set(targetRowBean, object);
		}
	}
	
	/**
	 * Gives the value of a Column in the given row
	 * @param row the current {@link Row}
	 * @param column Column
	 * @return the value of a {@link Column} in the given row, may be null
	 */
	public Object getValue(Row row, Column column) {
		Optional<RowKeySupport> foundKeyMapper = keyToField.keySet().stream().filter(c -> c.getColumn().equals(column)).findFirst();
		if (foundKeyMapper.isPresent()) {
			return row.get(foundKeyMapper.get().rowKey());
		} else {
			// we raise an exception (instead of returning null) because we think current usage is wrong
			throw new IllegalArgumentException("Column " + column + " is not mapped");
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
		// We transform the actual keyToField Map by a new one whose keys are took on the aliasProvider
		Map<RowKeySupport, IMutator> aliasToField = new HashMap<>(this.keyToField.size());
		this.keyToField.forEach((key, value) -> {
			Column column = key.getColumn();
			aliasToField.put(new RowKeySupport(column, aliasProvider), value);
		});
		ToBeanRowTransformer<T> result = new ToBeanRowTransformer<>(constructor, aliasToField);
		// listeners are given to the new instance because they may be interested to transform rows of this one
		result.rowTransformerListeners.addAll(rowTransformerListeners);
		return result;
	}
	
	
	public void addTransformerListener(TransformerListener<T> listener) {
		this.rowTransformerListeners.add(listener);
	}
	
	/**
	 * Internal small class to abstract how keys of the {@link Row} are given.
	 * Mainly created for {@link #withAliases(Function)} method
	 */
	private static class RowKeySupport {
		
		private final Column key;
		
		private final Function<Column, String> aliaser;
		
		private RowKeySupport(Column key) {
			this(key, Column::getName);
		}
		
		private RowKeySupport(Column key, Function<Column, String> aliaser) {
			this.key = key;
			this.aliaser = aliaser;
		}
		
		private Column getColumn() {
			return key;
		}
		
		private String rowKey() {
			return aliaser.apply(key);
		}
	}
	
	public interface TransformerListener<C> {
		
		void onTransform(C c, Row row);
		
	}
}
