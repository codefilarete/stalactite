package org.codefilarete.stalactite.sql;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class MySQLDMLNameProvier extends DMLNameProvider {
	
	/** MySQL keywords to be escape. TODO: to be completed */
	public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER, "key", "keys",
		"table", "index", "group", "cursor",
		"interval", "in"
	));
	
	public MySQLDMLNameProvier(Map<Table, String> tableAliases) {
		super(tableAliases);
	}
	
	@Override
	public String getSimpleName(@Nonnull Column column) {
		if (KEYWORDS.contains(column.getName())) {
			return "`" + column.getName() + "`";
		}
		return super.getSimpleName(column);
	}
	
	@Override
	public String getSimpleName(Table table) {
		if (KEYWORDS.contains(table.getName())) {
			return "`" + super.getSimpleName(table) + "`";
		}
		return super.getSimpleName(table);
	}
}
