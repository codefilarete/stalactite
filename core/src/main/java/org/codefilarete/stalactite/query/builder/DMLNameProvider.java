package org.codefilarete.stalactite.query.builder;

import java.util.Map;
import java.util.function.Function;

import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.Strings;

/**
 * A simple wrapper for methods that can give some necessary names during DML generation.
 * 
 * @author Guillaume Mary
 */
public class DMLNameProvider {
	
	private final Function<Fromable, String> tableAliaser;
	
	public DMLNameProvider(Function<Fromable, String> tableAliaser) {
		this.tableAliaser = tableAliaser;
	}
	
	public DMLNameProvider(Map<? extends Fromable, String> tableAliases) {
		this(tableAliases::get);
	}
	
	/**
	 * Gives a column name with table "path" (either alias or name according to {@link #getTablePrefix(Fromable)})
	 * @param column a column
	 * @return the column name prefixed with table name/alias
	 */
	public String getName(Selectable<?> column) {
		if (column instanceof JoinLink) {
			String tablePrefix = getTablePrefix(((JoinLink<?, ?>) column).getOwner());
			return tablePrefix + "." + getSimpleName(column);
		} else {
			return getSimpleName(column);
		}
	}
	
	/**
	 * Gives the column name (without table name).
	 * Aimed at being overridden to take keywords into account (and put it between quotes for instance)
	 * 
	 * @param column a column
	 * @return the column name (eventually escaped)
	 */
	public String getSimpleName(Selectable<?> column) {
		return column.getExpression();
	}
	
	public String getAlias(Fromable table) {
		return tableAliaser.apply(table);
	}
	
	public String getTablePrefix(Fromable table) {
		String tableAlias = getAlias(table);
		return Strings.isEmpty(tableAlias) ? getName(table) : tableAlias;
	}
	
	/**
	 * Gives the table name.
	 * Aimed at being overridden to take keywords into account (and put result between quotes for instance)
	 *
	 * @param table a table
	 * @return the table name (eventually escaped)
	 */
	public String getName(Fromable table) {
		return table.getAbsoluteName();
	}
}
