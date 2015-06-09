package org.gama.stalactite.persistence.sql.dml;

import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.mapping.StatementValues;
import org.gama.stalactite.persistence.sql.dml.binder.ParameterBinder;
import org.gama.stalactite.persistence.sql.dml.binder.ParameterBinderRegistry;
import org.gama.stalactite.persistence.sql.result.RowIterator;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Guillaume Mary
 */
public class SelectOperation extends CRUDOperation {
	
	/** Column indexes for where columns */
	private final Map<Column, Map.Entry<Integer, ParameterBinder>> whereIndexes;
	private final Map<String, ParameterBinder> columnBinders = new HashMap<>();

	/**
	 * 
	 * @param sql le SQL de selection, attendu que ce soit un select .. from .. where ..
	 * @param whereIndexes les indices des colonnes dans le where
	 * @param selectedColumns
	 */
	public SelectOperation(String sql, Map<Column, Integer> whereIndexes, List<Column> selectedColumns) {
		super(sql);
		this.whereIndexes = getBinders(whereIndexes);
		ParameterBinderRegistry currentDialect = PersistenceContext.getCurrent().getDialect().getParameterBinderRegistry();
		for (Column column : selectedColumns) {
			columnBinders.put(column.getName(), currentDialect.getBinder(column));
		}
	}
	
	public RowIterator execute() throws SQLException {
		ResultSet resultSet = getStatement().executeQuery();
		return new RowIterator(resultSet, columnBinders);
	}

	public Map<Column,Map.Entry<Integer,ParameterBinder>> getWhereIndexes() {
		return whereIndexes;
	}

	@Override
	protected void applyValues(StatementValues values) throws SQLException {
		applyWhereValues(whereIndexes, values);
	}
}
