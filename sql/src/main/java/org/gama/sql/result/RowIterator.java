package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;

/**
 * {@link ResultSetIterator} specialized in {@link Row} building for each Resulset line.
 *
 * @author Guillaume Mary
 */
public class RowIterator extends ResultSetIterator<Row> {
	
	private final ParameterBinderIndex<String> columnNameBinders;
	
	/**
	 * ResultSetIterator constructor
	 *
	 * @param columnNameBinders columns and associated {@link ParameterBinder} to use for <t>ResultSet</t> reading
	 */
	public RowIterator(Map<String, ParameterBinder> columnNameBinders) {
		this(null, columnNameBinders);
	}
	
	/**
	 * ResultSetIterator constructor
	 *
	 * @param rs a ResultSet to wrap into an <t>Iterator</t>
	 * @param columnNameBinders columns and associated {@link ParameterBinder} to use for <t>ResultSet</t> reading
	 */
	public RowIterator(ResultSet rs, Map<String, ParameterBinder> columnNameBinders) {
		this(rs, ParameterBinderIndex.fromMap(columnNameBinders));
	}
	
	public RowIterator(ResultSet rs, ParameterBinderIndex<String> columnNameBinders) {
		super(rs);
		this.columnNameBinders = columnNameBinders;
	}
	
	@Override
	public Row convert(ResultSet rs) throws SQLException {
		Row toReturn = new Row();
		for (Entry<String, ParameterBinder> columnEntry : columnNameBinders.all()) {
			String columnName = columnEntry.getKey();
			toReturn.put(columnName, columnEntry.getValue().get(columnName, rs));
		}
		return toReturn;
	}
}
