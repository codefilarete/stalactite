package org.gama.stalactite.query.builder;

import java.util.Map;

import org.gama.lang.Strings;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractDMLBuilder implements SQLBuilder {
	
	protected final Map<Table, String> tableAliases;

	public AbstractDMLBuilder(Map<Table, String> tableAliases) {
		this.tableAliases = tableAliases;
	}

	protected String getName(Column column) {
		String tablePrefix = getTablePrefix(column.getTable());
		return tablePrefix + "." + column.getName();
	}

	protected String getAlias(Table table) {
		return tableAliases.get(table);
	}

	protected String getTablePrefix(Table table) {
		String tableAlias = getAlias(table);
		return Strings.isEmpty(tableAlias) ? table.getName() : tableAlias;
	}
}