package org.codefilarete.stalactite.sql;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class MariaDBDMLNameProvier extends DMLNameProvider {
	
	/** MariaDB keywords to be escape. TODO: to be completed */
	public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER, "key", "keys",
		"table", "index", "group", "cursor",
		"interval", "in"
	));
	
	public MariaDBDMLNameProvier(Map<Table, String> tableAliases) {
		super(tableAliases);
	}
	
	@Override
	public String getSimpleName(@Nonnull Selectable<?> column) {
		if (KEYWORDS.contains(column.getExpression())) {
			return "`" + column.getExpression() + "`";
		}
		return super.getSimpleName(column);
	}
	
	@Override
	public String getName(Fromable table) {
		if (KEYWORDS.contains(table.getName())) {
			return "`" + super.getName(table) + "`";
		}
		return super.getName(table);
	}
}
