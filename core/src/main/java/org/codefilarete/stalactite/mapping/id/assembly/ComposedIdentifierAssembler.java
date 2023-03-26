package org.codefilarete.stalactite.mapping.id.assembly;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

/**
 * Describes the way a composed identifier is read and written to a database.
 *
 * @param <I> identifier type
 * @author Guillaume Mary
 * @see SimpleIdentifierAssembler
 */
public abstract class ComposedIdentifierAssembler<I, T extends Table<T>> implements IdentifierAssembler<I, T> {
	
	private final PrimaryKey<T, I> primaryKey;
	
	/**
	 * Constructor for cases where primary key columns are those of the table
	 * @param table any table (non null)
	 */
	protected ComposedIdentifierAssembler(T table) {
		this(table.getPrimaryKey());
	}
	
	/**
	 * Constructor which explicitly gets columns to be used for identifying a bean
	 * @param primaryKey some columns to be used to compose an identifier
	 */
	protected ComposedIdentifierAssembler(PrimaryKey<T, I> primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	@Override
	public Set<Column<T, Object>> getColumns() {
		return primaryKey.getColumns();
	}
	
	@Nullable
	@Override
	public abstract I assemble(Function<Column<?, ?>, Object> columnValueProvider);
	
	@Override
	public Map<Column<T, Object>, Object> getColumnValues(List<I> ids) {
		Map<Column<T, Object>, Object> pkValues = new HashMap<>();
		// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
		if (ids.size() == 1) {
			Map<Column<T, Object>, Object> localPkValues = getColumnValues(Iterables.first(ids));
			primaryKey.getColumns().forEach(pkColumn -> pkValues.put(pkColumn, localPkValues.get(pkColumn)));
		} else {
			ids.forEach(id -> {
				Map<Column<T, Object>, Object> localPkValues = getColumnValues(id);
				primaryKey.getColumns().forEach(pkColumn -> ((List<Object>) pkValues.computeIfAbsent(pkColumn, k -> new ArrayList<>())).add(localPkValues.get(pkColumn)));
			});
		}
		return pkValues;
	}
}
