package org.codefilarete.stalactite.mapping.id.assembly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * Describes the way a simple (single column) identifier is read and written to a database.
 *
 * @param <I> identifier type
 * @author Guillaume Mary
 * @see ComposedIdentifierAssembler
 */
public class SimpleIdentifierAssembler<I, T extends Table<T>> implements IdentifierAssembler<I, T> {
	
	private final Column<T, I> primaryKey;
	
	public SimpleIdentifierAssembler(Column<T, I> primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	public Column<T, I> getColumn() {
		return primaryKey;
	}
	
	@Override
	public Set<Column<T, ?>> getColumns() {
		return Arrays.asHashSet(primaryKey);
	}
	
	@Override
	public I assemble(Function<Column<?, ?>, Object> columnValueProvider) {
		Object value = columnValueProvider.apply(primaryKey);
		return (I) (value == null || isDefaultPrimitiveValue(value) ? null : value);
	}
	
	@Override
	public Map<Column<T, ?>, Object> getColumnValues(I id) {
		return Collections.singletonMap(primaryKey, id);
	}
	
	@Override
	public Map<Column<T, ?>, Object> getColumnValues(Iterable<I> ids) {
		Map<Column<T, I>, Object> pkValues = new HashMap<>();
		// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
		List<I> idsAsList = Iterables.asList(ids);
		if (idsAsList.size() == 1) {
			Map<Column<T, ?>, Object> localPkValues = getColumnValues(idsAsList.get(0));
			pkValues.put(primaryKey, localPkValues.get(primaryKey));
		} else {
			ids.forEach(id -> {
				Map<Column<T, ?>, Object> localPkValues = getColumnValues(id);
				((List<Object>) pkValues.computeIfAbsent(primaryKey, k -> new ArrayList<>())).add(localPkValues.get(primaryKey));
			});
		}
		return (Map) pkValues;
	}
	
	public static boolean isDefaultPrimitiveValue(Object o) {
		return Reflections.PRIMITIVE_DEFAULT_VALUES.get(o.getClass()) == o;
	}
}
