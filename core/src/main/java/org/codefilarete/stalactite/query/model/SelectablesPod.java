package org.codefilarete.stalactite.query.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * Contract for elements that can provide {@link org.codefilarete.stalactite.sql.ddl.structure.Column}s, and in a wider way, {@link Selectable}s,
 * considered items from a Select clause of an SQL query.
 * 
 * @author Guillaume Mary
 */
public interface SelectablesPod {
	
	/**
	 * Returns a read-only view of selected items
	 * 
	 * @return non modifiable
	 */
	Set<? extends Selectable<?>> getColumns();
	
	default <C extends Selectable<?>> C findColumn(String columnName) {
		for (Entry<? extends Selectable<?>, String> alias : getAliases().entrySet()) {
			if (alias.getValue().equals(columnName)) {
				return (C) alias.getKey();
			}
		}
		for (Selectable<?> column : getColumns()) {
			if (column instanceof JoinLink && column.getExpression().equals(columnName)) {
				return (C) column;
			}
		}
		return null;
	}
	
	default Map<String, ? extends Selectable<?>> mapColumnsOnName() {
		Map<String, Selectable<?>> result = new HashMap<>();
		for (Selectable<?> column : getColumns()) {
			if (column instanceof Column) {
				result.put(((Column) column).getName(), column);
			}
		}
		for (Entry<? extends Selectable<?>, String> alias : getAliases().entrySet()) {
			result.put(alias.getValue(), alias.getKey());
		}
		return result;
	}
	
	/**
	 * Gives column aliases. If a column as no alias then it may not be present in the result, so it is encouraged to use
	 * {@link Map#getOrDefault(Object, Object)} with {@link Column#getName()} as second argument if you need to know under which name a column is
	 * present in the select clause. At the opposite, a free expression such as an operator call should be present here, else it can only be accessed
	 * through its index.
	 * 
	 * @return selected elements with real alias
	 */
	Map<? extends Selectable<?>, String> getAliases();
}
