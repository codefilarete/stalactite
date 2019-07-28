package org.gama.stalactite.sql.result;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.sql.dml.SQLExecutionException;

/**
 * Reader of a multiple columns (by their name) from a {@link java.sql.ResultSet}.
 * Based on a collection of {@link SingleColumnReader}s.
 * 
 * @author Guillaume Mary
 */
public class MultipleColumnsReader<C> implements ColumnReader<C> {
	
	private final Set<SingleColumnReader> columnReaders;
	
	private final Function<Map<String, Object>, C> assembler;
	
	/**
	 * Default contructor
	 * 
	 * @param columnReaders readers to be used for reading {@link ResultSet} rows per column
	 * @param assembler function to assemble row values to create the bean of a row,
	 * will consume {@link ResultSet} values by the form of a column name to value mapping
	 */
	public MultipleColumnsReader(Set<SingleColumnReader> columnReaders, Function<Map<String, Object> /* ResultSet row values */, C> assembler) {
		this.columnReaders = columnReaders;
		this.assembler = assembler;
	}
	
	@Override
	public C read(@Nonnull ResultSet resultSet) throws SQLException {
		Map<String, Object> rowValues;
		try {
			rowValues = Iterables.map(columnReaders, SingleColumnReader::getColumnName, reader -> {
				try {
					return reader.read(resultSet);
				} catch (SQLException e) {
					// we encapsulate it to deencapsulate it in the global catch caluse, kind of weird but necessary due to non thorwing function in streams
					throw new SQLExecutionException(e);
				}
			});
		} catch (SQLExecutionException e) {
			// deecapsulating error
			throw (SQLException) e.getCause();
		}
		return assembler.apply(rowValues);
	}
	
	
	@Override
	public MultipleColumnsReader<C> copyWithAliases(Function<String, String> columnMapping) {
		Set<SingleColumnReader> resultColumnReaders = Iterables.collect(columnReaders, r -> r.copyWithAliases(columnMapping), HashSet::new);
		return new MultipleColumnsReader<>(resultColumnReaders, assembler);
	}
}
