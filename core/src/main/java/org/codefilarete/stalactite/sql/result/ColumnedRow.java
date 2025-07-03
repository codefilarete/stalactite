package org.codefilarete.stalactite.sql.result;

import org.codefilarete.stalactite.query.model.Selectable;

/**
 * Contract to read data of an underlying {@link java.sql.ResultSet} from a {@link Selectable} (abstract representation
 * of database a column).
 * 
 * @author Guillaume Mary
 */
public interface ColumnedRow {

	/**
	 * Returns the value of the given column of the underlying {@link java.sql.ResultSet}
	 * 
	 * @param column the column to get a value from
	 * @return the column data
	 * @param <E> the column data type
	 */
	<E> E get(Selectable<E> column);
}
