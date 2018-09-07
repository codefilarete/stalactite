package org.gama.stalactite.query.builder;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.function.Function;

import org.gama.lang.Strings;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A simple wrapper for methods that can give some necessary names during DML generation.
 * 
 * @author Guillaume Mary
 */
public class DMLNameProvider {
	
	private final Function<Table, String> tableAliases;
	
	public DMLNameProvider(Function<Table, String> tableAliases) {
		this.tableAliases = tableAliases;
	}
	
	public DMLNameProvider(Map<? extends Table, String> tableAliases) {
		this(tableAliases::get);
	}
	
	/**
	 * Gives a column name with table "path" (either alias or name according to {@link #getTablePrefix(Table)})
	 * @param column a column
	 * @return the column name prefixed with table name/alias
	 */
	public String getName(@Nonnull Column column) {
		String tablePrefix = getTablePrefix(column.getTable());
		return tablePrefix + "." + getSimpleName(column);
	}
	
	/**
	 * Gives the column name (without table name).
	 * Aimed at being overriden to take key words into account (and put it between quotes for instance)
	 * 
	 * @param column a column
	 * @return the column name (eventually escaped)
	 */
	public String getSimpleName(@Nonnull Column column) {
		return column.getName();
	}
	
	public String getAlias(Table table) {
		return tableAliases.apply(table);
	}
	
	public String getTablePrefix(Table table) {
		String tableAlias = getAlias(table);
		return Strings.isEmpty(tableAlias) ? getSimpleName(table) : tableAlias;
	}
	
	/**
	 * Gives the table name.
	 * Aimed at being overriden to take key words into account (and put it between quotes for instance)
	 *
	 * @param table a table
	 * @return the table name (eventually escaped)
	 */
	public String getSimpleName(Table table) {
		return table.getAbsoluteName();
	}
}
