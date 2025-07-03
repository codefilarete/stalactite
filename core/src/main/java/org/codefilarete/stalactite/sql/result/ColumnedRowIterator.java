package org.codefilarete.stalactite.sql.result;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;

/**
 * {@link ResultSetIterator} specialized in {@link ColumnedRow} building for each {@link ResultSet} line.
 *
 * @author Guillaume Mary
 */
public class ColumnedRowIterator extends ResultSetIterator<ColumnedRow> {
	
	/** Readers for each column of the {@link ResultSet}, by name (may contain double but doesn't matter, causes only extra conversion) */
	private final Iterable<Decoder> decoders;

	private final Map<Selectable<?>, String> aliases;
	
	/**
	 * Constructs an instance without {@link ResultSet} : it shall be set further with {@link #setResultSet(ResultSet)}.
	 *
	 * @param columnNameBinders columns and associated {@link ResultSetReader} to use for {@link ResultSet} reading
	 */
	public ColumnedRowIterator(Map<? extends Selectable<?>, ? extends ResultSetReader<?>> columnNameBinders,
							   Map<? extends Selectable<?>, String> aliases) {
		this(null, columnNameBinders, aliases);
	}
	
	/**
	 * Constructs an instance that will iterate over the given {@link ResultSet}. It can be changed with {@link #setResultSet(ResultSet)}.
	 *
	 * @param rs a ResultSet to wrap into an {@link java.util.Iterator}
	 * @param columnNameBinders column names and associated {@link ResultSetReader} to use for {@link ResultSet} reading
	 */
	public ColumnedRowIterator(@Nullable ResultSet rs,
							   Map<? extends Selectable<?>, ? extends ResultSetReader<?>> columnNameBinders,
							   Map<? extends Selectable<?>, String> aliases) {
		super(rs);
		this.decoders = Decoder.decoders(columnNameBinders.entrySet());
		this.aliases = (Map<Selectable<?>, String>) aliases;
	}
	
	/**
	 * Constructs an instance that will iterate over the given {@link ResultSet}. It can be changed with {@link #setResultSet(ResultSet)}.
	 * 
	 * @param rs a ResultSet to wrap into an {@link java.util.Iterator}
	 * @param columnNameBinders object to extract column names and associated {@link ResultSetReader} to use for <t>ResultSet</t> reading
	 */
	public ColumnedRowIterator(ResultSet rs, ParameterBinderIndex<? extends Selectable, ? extends ResultSetReader> columnNameBinders, Map<? extends Selectable<?>, String> aliases) {
		super(rs);
		this.decoders = Decoder.decoders(columnNameBinders.all());
		this.aliases = (Map<Selectable<?>, String>) aliases;
	}
	
	/**
	 * Implementation that converts current {@link ResultSet} line into a {@link Row} according to {@link ResultSetReader}s given at construction time.
	 * 
	 * @param rs {@link ResultSet} positioned at line that must be converted
	 * @return a {@link Row} containing values given by {@link ResultSetReader}s
	 * @throws SQLException if a read error occurs
	 * @throws BindingException if a binding doesn't match its ResultSet value
	 */
	@Override
	public ColumnedRow convert(ResultSet rs) throws SQLException {
		MapBasedColumnedRow toReturn = new MapBasedColumnedRow();
		for (Decoder columnEntry : decoders) {
			Selectable<?> column = columnEntry.getColumn();
			Object columnValue = columnEntry.getReader().get(rs, aliases.get(column));
			toReturn.put(column, columnValue);
		}
		return toReturn;
	}
	
	/**
	 * Simple storage of mapping between column name and their {@link ResultSetReader}
	 */
	private static class Decoder {
		
		private static Iterable<Decoder> decoders(Iterable<? extends Map.Entry<? extends Selectable, ? extends ResultSetReader>> input) {
			Set<Decoder> result = new LinkedHashSet<>();
			input.forEach(e -> result.add(new Decoder(e.getKey(), e.getValue())));
			return result;
		}
		
		private final Selectable<?> column;
		
		private final ResultSetReader<?> reader;
		
		private Decoder(Selectable<?> column, ResultSetReader<?> reader) {
			this.column = column;
			this.reader = reader;
		}
		
		private Selectable<?> getColumn() {
			return column;
		}
		
		private ResultSetReader<?> getReader() {
			return reader;
		}
	}
}
