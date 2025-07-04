package org.codefilarete.stalactite.mapping.id.assembly;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Describes the way an identifier is read and written to a database.
 * Created to handle composed id.
 *
 * @param <I> identifier type
 * @param <T> table type this assembler read columns from
 * @author Guillaume Mary
 */
public interface IdentifierAssembler<I, T extends Table<T>> {
	
	/**
	 * Creates an identifier from a {@link Row}
	 *
	 * @param row a current row of an {@link java.sql.ResultSet}
	 * @return an identifier
	 */
	I assemble(ColumnedRow row);
	
	Set<Column<T, ?>> getColumns();
	
	/**
	 * Gives identifier values for each primary key column
	 * 
	 * @param id an identifier
	 * @return a {@link Map} which keys are primary key columns and values are column-values of the identifier
	 */
	Map<Column<T, ?>, ?> getColumnValues(I id);
	
	/**
	 * A Collection-form of {@link #getColumnValues(Object)}.
	 * Should give, for each primary key column, all values of the given identifiers. So result should be a {@code Map<Column, List>}, but, due to
	 * potential misunderstanding in {@link org.codefilarete.stalactite.sql.statement.ExpandableStatement} if ids is single (result must contain
	 * single Object instead of {@link List}), result signature is {@code Map<Column, Object>}
	 * 
	 * @param ids identifiers
	 * @return a {@link Map} which keys are primary key columns and values are column-values of all identifiers 
	 */
	Map<Column<T, ?>, ?> getColumnValues(Iterable<I> ids);
	
}
