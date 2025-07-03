package org.codefilarete.stalactite.mapping;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.FieldIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Class for transforming columns into a bean.
 * 
 * @author Guillaume Mary
 */
public class ToBeanRowTransformer<C> extends AbstractTransformer<C> {
	
	private final Map<Column<?, ?>, Mutator<C, Object>> columnToMember;
	
	/**
	 * A constructor that maps all fields of a class by name
	 *
	 * @param clazz instance type to be built
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
	 * @param clazz the constructor to be used to instantiate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Class<C> clazz, Map<? extends Column<?, ?>, ? extends Mutator<C, Object>> columnToMember) {
		super(clazz);
		this.columnToMember = (Map<Column<?, ?>, Mutator<C, Object>>) columnToMember;
	}
	
	/**
	 *
	 * @param beanFactory factory to be used to instantiate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Function<? extends Function<Column<?, ?>, Object>, C> beanFactory,
								Map<? extends Column<?, ?>, ? extends Mutator<C, Object>> columnToMember) {
		super(beanFactory);
		this.columnToMember = (Map<Column<?, ?>, Mutator<C, Object>>) columnToMember;
	}
	
	protected ToBeanRowTransformer(Function<Function<Column<?, ?>, Object>, C> beanFactory,
								   Map<? extends Column<?, ?>, ? extends Mutator<C, Object>> columnToMember,
								   Collection<TransformerListener<C>> rowTransformerListeners) {
		super(beanFactory, rowTransformerListeners);
		this.columnToMember = (Map<Column<?, ?>, Mutator<C, Object>>) columnToMember;
	}
	
	public Map<Column<?, ?>, Mutator<C, Object>> getColumnToMember() {
		return columnToMember;
	}
	
	@Override
	public void applyRowToBean(ColumnedRow source, C targetRowBean) {
		for (Entry<Column<?, ?>, Mutator<C, Object>> columnFieldEntry : columnToMember.entrySet()) {
			Object propertyValue = source.get(columnFieldEntry.getKey());
			applyValueToBean(targetRowBean, columnFieldEntry, propertyValue);
		}
	}
	
	protected void applyValueToBean(C targetRowBean, Entry<? extends Column, ? extends Mutator<C, Object>> columnFieldEntry, Object propertyValue) {
		columnFieldEntry.getValue().set(targetRowBean, propertyValue);
	}
}
