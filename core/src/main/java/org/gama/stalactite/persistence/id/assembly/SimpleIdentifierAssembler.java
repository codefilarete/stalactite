package org.gama.stalactite.persistence.id.assembly;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Describes the way a simple (single column) identifier is read and written to a database.
 *
 * @param <I> identifier type
 * @author Guillaume Mary
 * @see ComposedIdentifierAssembler
 */
public class SimpleIdentifierAssembler<I> implements IdentifierAssembler<I> {
	
	private final Column<Table, I> primaryKey;
	
	public <T extends Table> SimpleIdentifierAssembler(Column<T, I> primaryKey) {
		this.primaryKey = (Column) primaryKey;
	}
	
	public Column<Table, I> getColumn() {
		return primaryKey;
	}
	
	@Override
	public I assemble(@Nonnull Row row, @Nonnull ColumnedRow rowAliaser) {
		return (I) rowAliaser.getValue(primaryKey, row);
	}
	
	@Override
	public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull I id) {
		return Collections.singletonMap((Column) primaryKey, id);
	}
	
	@Override
	public <T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull List<I> ids) {
		Map<Column<Table, I>, Object> pkValues = new HashMap<>();
		// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
		if (ids.size() == 1) {
			Map<Column<T, Object>, Object> localPkValues = getColumnValues(Iterables.first(ids));
			pkValues.put(primaryKey, localPkValues.get(primaryKey));
		} else {
			ids.forEach(id -> {
				Map<Column<T, Object>, Object> localPkValues = getColumnValues(id);
				((List<Object>) pkValues.computeIfAbsent(primaryKey, k -> new ArrayList<>())).add(localPkValues.get(primaryKey));
			});
		}
		return (Map) pkValues;
	}
}
