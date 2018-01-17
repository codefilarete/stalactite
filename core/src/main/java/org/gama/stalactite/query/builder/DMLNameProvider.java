package org.gama.stalactite.query.builder;

import java.util.Map;

import org.gama.lang.Strings;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A simple wrapper for methods that can give some necessary names during DML generation.
 * 
 * @author Guillaume Mary
 */
public class DMLNameProvider {
	
	protected final Map<Table, String> tableAliases;
	
	public DMLNameProvider(Map<Table, String> tableAliases) {
		this.tableAliases = tableAliases;
	}
	
	public String getName(Column column) {
		String tablePrefix = getTablePrefix(column.getTable());
		return tablePrefix + "." + column.getName();
	}
	
	public String getAlias(Table table) {
		return tableAliases.get(table);
	}
	
	public String getTablePrefix(Table table) {
		String tableAlias = getAlias(table);
		return Strings.isEmpty(tableAlias) ? table.getName() : tableAlias;
	}
}
