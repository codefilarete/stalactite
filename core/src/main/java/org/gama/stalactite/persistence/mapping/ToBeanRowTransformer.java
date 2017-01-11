package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.gama.reflection.IMutator;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Class for transforming columns into a bean.
 * 
 * @author Guillaume Mary
 */
public class ToBeanRowTransformer<T> extends AbstractTransformer<T> {
	
	private final Map<RowKeyMapper, IMutator> keyToField;
	
	/**
	 * A constructor that maps all fields of a class by name
	 *
	 * @param clazz the instances class to be build
	 * @param table the mapped table
	 * @param warnOnMissingColumn indicates if an Exception must be thrown when no {@link Column} in the {@link Table} can be found for a field
	 * (matching name)
	 */
	public ToBeanRowTransformer(Class<T> clazz, Table table, boolean warnOnMissingColumn) {
		this(Reflections.getDefaultConstructor(clazz), new HashMap<>(10), true);
		Map<String, Column> columnPerName = table.mapColumnsOnName();
		FieldIterator fieldIterator = new FieldIterator(clazz);
		Iterables.visit(fieldIterator, new ForEach<Field, Void>() {
			@Override
			public Void visit(Field field) {
				Column column = columnPerName.get(field.getName());
				if (column == null) {
					if (warnOnMissingColumn) {
						throw new UnsupportedOperationException("Missing column for field " + field.getDeclaringClass().getName() + "." + field.getName());
					}
				} else {
					keyToField.put(new ColumnRowKey(column), PropertyAccessor.mutator(field.getDeclaringClass(), field.getName()));
				}
				return null;
			}
		});
	}
	
	/**
	 *
	 * @param constructor the constructor to be used to instanciate a new bean
	 * @param columnToField the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Constructor<T> constructor, Map<Column, IMutator> columnToField) {
		this(constructor, new HashMap<>(10), true);
		columnToField.entrySet().forEach(e -> keyToField.put(new ColumnRowKey(e.getKey()), e.getValue()));
	}
	
	/**
	 * 
	 * @param constructor the constructor to be used to instanciate a new bean
	 * @param keyToField the mapping between key in rows and helper that fixes values of the bean
	 * @param constructorProof a mark to distinguish this constructor from {@link #ToBeanRowTransformer(Constructor, Map)} due to type erasure
	 */
	private ToBeanRowTransformer(Constructor<T> constructor, Map<RowKeyMapper, IMutator> keyToField, boolean constructorProof) {
		super(constructor);
		this.keyToField = keyToField;
	}
	
	@Override
	public void applyRowToBean(Row source, T targetRowBean) {
		for (Entry<RowKeyMapper, IMutator> columnFieldEntry : keyToField.entrySet()) {
			String propertyName = columnFieldEntry.getKey().rowKey();
			Object object = source.get(propertyName);
			columnFieldEntry.getValue().set(targetRowBean, object);
		}
	}
	
	/**
	 * Allows to change the mapping used by this instance by a new one. A new instance is returned that will read keys according to the given function  
	 * @param aliasProvider a function that gives new row keys than the default ones
	 * @return a new instance of {@link ToBeanRowTransformer} whose read keys are those given by the function
	 */
	public ToBeanRowTransformer<T> withAliases(Function<Column, String> aliasProvider) {
		// We transform the actual keyToField Map by a new one whose keys are took on the aliasProvider
		Map<RowKeyMapper, IMutator> aliasToField = new HashMap<>(this.keyToField.size());
		this.keyToField.entrySet().forEach(e -> aliasToField.put(
				new StringRowKey(aliasProvider.apply(((ColumnRowKey) e.getKey()).getColumn())), 
				e.getValue())
		);
		return new ToBeanRowTransformer<>(constructor, aliasToField, true);
	}
	
	/**
	 * A short interface used to abstract how keys of the {@link Row} are given.
	 * Mainly created for {@link #withAliases(Function)} method
	 */
	private interface RowKeyMapper {
		
		String rowKey();
		
	}
	
	/**
	 * Trivial {@link RowKeyMapper} when alias is given
	 */
	private static class StringRowKey implements RowKeyMapper {
		
		private final String alias;
		
		private StringRowKey(String alias) {
			this.alias = alias;
		}
		
		@Override
		public String rowKey() {
			return alias;
		}
	}
	
	/**
	 * {@link RowKeyMapper} who takes the key on the column name
	 */
	private static class ColumnRowKey implements RowKeyMapper {
		
		private final Column key;
		
		private ColumnRowKey(Column key) {
			this.key = key;
		}
		
		public Column getColumn() {
			return key;
		}
		
		@Override
		public String rowKey() {
			return key.getName();
		}
	}
}
