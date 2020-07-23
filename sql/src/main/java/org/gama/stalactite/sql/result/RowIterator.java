package org.gama.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

import org.gama.stalactite.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.sql.binder.ResultSetReader;
import org.gama.stalactite.sql.dml.SQLStatement.BindingException;

/**
 * {@link ResultSetIterator} specialized in {@link Row} building for each {@link ResultSet} line.
 *
 * @author Guillaume Mary
 */
public class RowIterator extends ResultSetIterator<Row> {
	
	/** Readers for each column of the RestulSet, by name (may contains double but doesn't matter, causes only extra conversion) */
	private final Iterable<Decoder> decoders;
	
	/**
	 * Constructs an instance without {@link ResultSet} : it shall be set further with {@link #setResultSet(ResultSet)}.
	 *
	 * @param columnNameBinders columns and associated {@link ResultSetReader} to use for {@link ResultSet} reading
	 */
	public RowIterator(Map<String, ? extends ResultSetReader> columnNameBinders) {
		this(null, columnNameBinders);
	}
	
	/**
	 * Constructs an instance that will iterate over the given {@link ResultSet}. It can be changed with {@link #setResultSet(ResultSet)}.
	 *
	 * @param rs a ResultSet to wrap into an {@link java.util.Iterator}
	 * @param columnNameBinders column names and associated {@link ResultSetReader} to use for {@link ResultSet} reading
	 */
	public RowIterator(ResultSet rs, Map<String, ? extends ResultSetReader> columnNameBinders) {
		super(rs);
		decoders = Decoder.decoders(columnNameBinders.entrySet());
	}
	
	/**
	 * Constructs an instance that will iterate over the given {@link ResultSet}. It can be changed with {@link #setResultSet(ResultSet)}.
	 * 
	 * @param rs a ResultSet to wrap into an {@link java.util.Iterator}
	 * @param columnNameBinders object to extract column names and associated {@link ResultSetReader} to use for <t>ResultSet</t> reading
	 */
	public RowIterator(ResultSet rs, ParameterBinderIndex<String, ? extends ResultSetReader> columnNameBinders) {
		super(rs);
		decoders = Decoder.decoders(columnNameBinders.all());
	}
	
	/**
	 * Implementation that converts current {@link ResultSet} line into a {@link Row} according to {@link ResultSetReader}s given at construction time.
	 * 
	 * @param rs {@link ResultSet} positionned at line that must be converted
	 * @return a {@link Row} containing values given by {@link ResultSetReader}s
	 * @throws SQLException if a read error occurs
	 * @throws BindingException if a binding doesn't match its ResultSet value
	 */
	@Override
	public Row convert(ResultSet rs) throws SQLException {
		Row toReturn = new Row();
		for (Decoder columnEntry : decoders) {
			String columnName = columnEntry.getColumnName();
			Object columnValue = columnEntry.getReader().get(rs, columnName);
			toReturn.put(columnName, columnValue);
		}
		return toReturn;
	}
	
	/**
	 * Simple storage of mapping between column name and their {@link ResultSetReader}
	 */
	private static class Decoder {
		
		private static Iterable<Decoder> decoders(Iterable<? extends Map.Entry<String, ? extends ResultSetReader>> input) {
			// NB: we don't expect duplicate in entries column names, so we don't apply any case insensitive sort
			TreeSet<Decoder> result = new TreeSet<>(Comparator.comparing(Decoder::getColumnName));
			input.forEach(e -> result.add(new Decoder(e.getKey(), e.getValue())));
			return result;
		}
		
		private final String columnName;
		
		private final ResultSetReader reader;
		
		private Decoder(String columnName, ResultSetReader reader) {
			this.columnName = columnName;
			this.reader = reader;
		}
		
		private String getColumnName() {
			return columnName;
		}
		
		private ResultSetReader getReader() {
			return reader;
		}
	}
}
