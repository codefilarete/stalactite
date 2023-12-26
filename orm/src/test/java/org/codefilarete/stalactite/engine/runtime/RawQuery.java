package org.codefilarete.stalactite.engine.runtime;

import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.Arrays;

/**
 * Very dumb parser that extract selected columns from a raw SQL Query.
 * Made to prevent from depending of {@link Column} equals/hashCode and let assert on column as Set of Strings
 *
 * @author Guillaume Mary
 */
public class RawQuery {
	
	private final Set<String> columns = new HashSet<>();
	private final String from;
	
	public RawQuery(String sql) {
		int selectStartIndex = sql.indexOf("select ");
		int fromStartIndex = sql.indexOf(" from ");
		String rawSelect = sql.substring(selectStartIndex + 7, fromStartIndex);
		columns.addAll(Arrays.asHashSet(rawSelect.split("\\s*,\\s*")));
		from = sql.substring(fromStartIndex + 6);
	}
	
	public Set<String> getColumns() {
		return columns;
	}
	
	public String getFrom() {
		return from;
	}
}
