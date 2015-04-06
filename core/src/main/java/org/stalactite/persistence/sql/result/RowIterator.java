package org.stalactite.persistence.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.sql.dml.binder.ParameterBinder;
import org.stalactite.persistence.sql.dml.binder.ParameterBinderRegistry;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class RowIterator extends ResultSetIterator<Row> {
	
	private Map<String, ParameterBinder> columnNameBinders;

	/**
	 * Constructeur de ResultSetIterator.
	 *
	 * @param rs un ResultSet à encapsuler dans un <t>Iterator</t>
	 * @param columnsToRead les colonnes à lire dans le <t>ResultSet</t>
	 */
	public RowIterator(ResultSet rs, Iterable<Table.Column> columnsToRead) throws SQLException {
		this(rs, new HashMap<String, ParameterBinder>());
		buildBinders(columnsToRead);
	}

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
	
	protected void buildBinders(Iterable<Column> columnsToRead) {
		ParameterBinderRegistry currentDialect = PersistenceContext.getCurrent().getDialect().getParameterBinderRegistry();
		for (Column column : columnsToRead) {
			columnNameBinders.put(column.getName(), currentDialect.getBinder(column));
		}
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
