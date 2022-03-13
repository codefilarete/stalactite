package org.codefilarete.stalactite.persistence.mapping;

import javax.annotation.Nullable;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * Class aimed at giving values of a {@link Row} from a {@link Column}, see {@link #getValue(Column, Row)}.
 * By default values are took in the {@link Row} by {@link Column} name, but they may also be given by any aliasing function when using
 * {@link #ColumnedRow(Function)}.
 * 
 * @author Guillaume Mary
 * @see #getValue(Column, Row) 
 */
public class ColumnedRow {
	
	private final Function<Column, String> aliasProvider;
	
	/**
	 * Default constructor, will take values in {@link Row}s by column names
	 */
	public ColumnedRow() {
		this(Column::getName);
	}
	
	/**
	 * Constructor that will apply the given "sliding" function on column names in order to make them match {@link Row} keys
	 * @param aliasProvider a {@link Function} that makes mapping between {@link Column} and {@link Row} keys
	 */
	public ColumnedRow(Function<Column, String> aliasProvider) {
		this.aliasProvider = aliasProvider;
	}
	
	/**
	 * Gives the value of the given {@link Column} from the given {@link Row}.
	 * Applies the alias function.
	 * 
	 * @param column any {@link Column}, expected to have a matching value in {@code row} (through the aliasing function)
	 * @param row any {@link Row}
	 * @param <T> {@link Column}'s table type
	 * @param <O> expected value type
	 * @return the value found in {@link Row} for the given {@link Column}, maybe null
	 */
	@Nullable
	public <T extends Table<T>, O> O getValue(Column<T, O> column, Row row) {
		String columnAlias = aliasProvider.apply(column);
		if (columnAlias != null) {
			return (O) row.get(columnAlias);
		} else {
			// we raise an exception (instead of returning null) because we think current usage is wrong
			throw new IllegalArgumentException("Column " + column + " has no matching alias in result set");
		}
	}
}
