package org.gama.stalactite.persistence.id.assembly;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.collection.Iterables;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Describes the way a composed identifier is read and written to a database.
 *
 * @param <I> identifier type
 * @author Guillaume Mary
 * @see SimpleIdentifierAssembler
 */
public abstract class ComposedIdentifierAssembler<I> implements IdentifierAssembler<I> {
	
	private final Set<Column> primaryKeyColumns;
	
	/**
	 * Constructor for cases where primary key columns are those of the table
	 * @param table any table (non null)
	 */
	protected ComposedIdentifierAssembler(@Nonnull Table table) {
		this(table.getPrimaryKey().getColumns());
	}
	
	/**
	 * Constructor which explicitly gets columns to be used for identifying a bean
	 * @param primaryKeyColumns some columns to be used to compose an identifier
	 */
	protected ComposedIdentifierAssembler(Set<Column> primaryKeyColumns) {
		this.primaryKeyColumns = primaryKeyColumns;
	}
	
	@Override
	public I assemble(@Nonnull Row row, @Nonnull ColumnedRow rowAliaser) {
		Map<Column, Object> primaryKeyElements = new HashMap<>();
		for (Column column : primaryKeyColumns) {
			primaryKeyElements.put(column, rowAliaser.getValue(column, row));
		}
		return assemble(primaryKeyElements);
	}
	
	/**
	 * Build an identifier from the given {@link java.sql.ResultSet} row extract.
	 * Expected to return null if the identifier can't be build
	 * 
	 * @param primaryKeyElements primary key values from a {@link java.sql.ResultSet} row
	 * @return a new identifier, null if it can't be built (given values are null for instance)
	 */
	@Nullable
	protected abstract I assemble(Map<Column, Object> primaryKeyElements);
	
	@Override
	public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull List<I> ids) {
		Map<Column<T, Object>, Object> pkValues = new HashMap<>();
		// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
		if (ids.size() == 1) {
			Map<Column<T, Object>, Object> localPkValues = getColumnValues(Iterables.first(ids));
			primaryKeyColumns.forEach(pkColumn -> pkValues.put(pkColumn, localPkValues.get(pkColumn)));
		} else {
			ids.forEach(id -> {
				Map<Column<T, Object>, Object> localPkValues = getColumnValues(id);
				primaryKeyColumns.forEach(pkColumn -> ((List<Object>) pkValues.computeIfAbsent(pkColumn, k -> new ArrayList<>())).add(localPkValues.get(pkColumn)));
			});
		}
		return pkValues;
	}
}
