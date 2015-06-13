package org.gama.stalactite.persistence.sql.dml;

import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.mapping.StatementValues;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
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
	 * @param sql the select SQL, expected to be select .. from .. where ..
	 * @param whereIndexes indexes of clumns in the where part
	 * @param selectedColumns selected columns in the select part
	 */
	public SelectOperation(String sql, Map<Column, Integer> whereIndexes, List<Column> selectedColumns) {
		super(sql);
		this.whereIndexes = getBinders(whereIndexes);
		ColumnBinderRegistry currentDialect = PersistenceContext.getCurrent().getDialect().getColumnBinderRegistry();
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
		// only where part can contains parameters, so only apply this part
		applyWhereValues(whereIndexes, values);
	}
}
