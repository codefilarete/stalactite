package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.sql.binder.ResultSetReader;

/**
 * {@link ResultSetIterator} specialized in {@link Row} building for each Resulset line.
 *
 * @author Guillaume Mary
 */
public class RowIterator extends ResultSetIterator<Row> {
	
	/** Readers for each column of the RestulSet, by name (may contains double but doesn't matter, causes only extra conversion) */
	private final Iterable<Decoder> decoders;
	
	/**
	 * ResultSetIterator constructor
	 *
	 * @param columnNameBinders columns and associated {@link ParameterBinder} to use for <t>ResultSet</t> reading
	 */
	public RowIterator(Map<String, ParameterBinder> columnNameBinders) {
		this(null, columnNameBinders);
	}
	
	/**
	 * RowIterator constructor
	 *
	 * @param rs a ResultSet to wrap into an <t>Iterator</t>
	 * @param columnNameBinders column names and associated {@link ParameterBinder} to use for <t>ResultSet</t> reading
	 */
	public RowIterator(ResultSet rs, Map<String, ? extends ResultSetReader> columnNameBinders) {
		super(rs);
		decoders = Decoder.decoders(columnNameBinders.entrySet());
	}
	
	/**
	 * Another way to build a {@link RowIterator}.
	 * 
	 * @param rs a ResultSet to wrap into an <t>Iterator</t>
	 * @param columnNameBinders object to extract column names and associated {@link ParameterBinder} to use for <t>ResultSet</t> reading
	 */
	public RowIterator(ResultSet rs, ParameterBinderIndex<String> columnNameBinders) {
		super(rs);
		Set<? extends Entry<String, ? extends ResultSetReader>> all = columnNameBinders.all();
		decoders = Decoder.decoders(all);
	}
	
	@Override
	public Row convert(ResultSet rs) throws SQLException {
		Row toReturn = new Row();
		for (Decoder columnEntry : decoders) {
			String columnName = columnEntry.getColumnName();
			toReturn.put(columnName, columnEntry.getReader().get(rs, columnName));
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
