package org.codefilarete.stalactite.persistence.mapping;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.FieldIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Class for transforming columns into a bean.
 * 
 * @author Guillaume Mary
 */
public class ToBeanRowTransformer<C> extends AbstractTransformer<C> {
	
	private final Map<Column, Mutator> columnToMember;
	
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
	 * @param clazz the constructor to be used to instanciate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Class<C> clazz, Map<Column, Mutator> columnToMember) {
		super(clazz);
		this.columnToMember = columnToMember;
	}
	
	/**
	 *
	 * @param beanFactory factory to be used to instanciate a new bean
	 * @param columnToMember the mapping between key in rows and helper that fixes values of the bean
	 */
	public ToBeanRowTransformer(Function<Function<Column, Object>, C> beanFactory, Map<Column, Mutator> columnToMember) {
		super(beanFactory, new ColumnedRow());
		this.columnToMember = columnToMember;
	}
	
	protected ToBeanRowTransformer(Function<Function<Column, Object>, C> beanFactory, Map<Column, Mutator> columnToMember,
								   ColumnedRow columnedRow, Collection<TransformerListener<C>> rowTransformerListeners) {
		super(beanFactory, columnedRow, rowTransformerListeners);
		this.columnToMember = columnToMember;
	}
	
	public Map<Column, Mutator> getColumnToMember() {
		return columnToMember;
	}
	
	@Override
	public void applyRowToBean(Row source, C targetRowBean) {
		for (Entry<Column, Mutator> columnFieldEntry : columnToMember.entrySet()) {
			Object propertyValue = getColumnedRow().getValue(columnFieldEntry.getKey(), source);
			applyValueToBean(targetRowBean, columnFieldEntry, propertyValue);
		}
	}
	
	protected void applyValueToBean(C targetRowBean, Entry<Column, Mutator> columnFieldEntry, Object propertyValue) {
		columnFieldEntry.getValue().set(targetRowBean, propertyValue);
	}
	
	/**
	 * Allows to change the mapping used by this instance to a new one. A new instance is returned that will read keys according to the given
	 * "sliding" function.
	 * Helpfull to reuse a {@link RowTransformer} over multiple queries which different column aliases.
	 * 
	 * @param columnedRow a wrapper that gives {@link Row} values by {@link Column}.
	 * @return a new instance of {@link ToBeanRowTransformer} which read keys are those given by the function
	 */
	public ToBeanRowTransformer<C> copyWithAliases(ColumnedRow columnedRow) {
		return new ToBeanRowTransformer<>(beanFactory,
				new HashMap<>(this.columnToMember),
				columnedRow,
				// listeners are given to the new instance because they may be interested in transforming rows of this one
				getRowTransformerListeners()
		);
	}
}
