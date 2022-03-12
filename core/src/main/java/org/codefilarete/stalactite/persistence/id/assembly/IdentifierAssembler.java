package org.codefilarete.stalactite.persistence.id.assembly;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.persistence.mapping.ColumnedRow;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;

/**
 * Describes the way an identifier is read and written to a database.
 * Created to handle composed id.
 *
 * @param <I> identifier type
 * @author Guillaume Mary
 */
public interface IdentifierAssembler<I> {
	
	/**
	 * Creates an identifier from a {@link Row}
	 *
	 * @param row a current row of an {@link java.sql.ResultSet}
	 * @param rowAliaser the expected decoder of the row (taking aliases into account).
	 * 			Implementation note : its {@link ColumnedRow#getValue(Column, Row)} should be used
	 * @return an identifier
	 */
	I assemble(@Nonnull Row row, @Nonnull ColumnedRow rowAliaser);
	
	/**
	 * Gives identifier values for each primary key column
	 * 
	 * @param id an identifier
	 * @param <T> table type
	 * @return a {@link Map} which keys are primary key columns and values are column-values of the identifier
	 */
	<T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull I id);
	
	/**
	 * A Collection-form of {@link #getColumnValues(Object)}.
	 * Should give, for each primary key column, all values of the given identifiers. So result should be a {@code Map<Column, List>}, but, due to
	 * potential misunderstanding in {@link org.codefilarete.stalactite.sql.dml.ExpandableStatement}, if ids is single, result must contains single instead of {@link List}
	 * so result signature is {@code Map<Column, Object>}
	 * 
	 * @param ids identifiers
	 * @param <T> table type
	 * @return a {@link Map} which keys are primary key columns and values are column-values of all identifiers 
	 */
	<T extends Table<T>> Map<Column<T, Object>, Object> getColumnValues(@Nonnull List<I> ids);
	
}
