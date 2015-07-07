package org.gama.sql.result;

import org.gama.lang.exception.Exceptions;
import org.gama.sql.binder.ParameterBinder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * {@link ResultSetIterator} specialized in {@link Row} building for each Resulset line.
 *
 * @author Guillaume Mary
 */
public class RowIterator extends ResultSetIterator<Row> {
	
	private final Map<String, ParameterBinder> columnNameBinders;
	
	/**
	 * Constructeur de ResultSetIterator.
	 *
	 * @param rs un ResultSet à encapsuler dans un <t>Iterator</t>
	 * @param columnNameBinders les colonnes et {@link ParameterBinder} à utiliser pour lire le <t>ResultSet</t>
	 */
	public RowIterator(ResultSet rs, Map<String, ParameterBinder> columnNameBinders) {
		super(rs);
		this.columnNameBinders = columnNameBinders;
	}
	
	@Override
	public Row convert(ResultSet rs) {
		Row toReturn = new Row();
		try {
			for (Entry<String, ParameterBinder> columnEntry : columnNameBinders.entrySet()) {
				String columnName = columnEntry.getKey();
				toReturn.put(columnName, columnEntry.getValue().get(columnName, rs));
			}
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return toReturn;
	}
}
