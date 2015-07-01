package org.gama.stalactite.persistence.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.util.Map;

/**
 * Specialization of {@link ColumnPreparedSQL} for select statement: gives access to selected columns thru {@link #getSelectParameterBinders()}
 * 
 * @author Guillaume Mary
 */
public class PreparedSelect extends ColumnPreparedSQL {
	
	private final Map<String, ParameterBinder> selectParameterBinders;
	
	public PreparedSelect(String sql, Map<Column, int[]> columnIndexes, Map<Column, ParameterBinder> parameterBinders, Map<String, ParameterBinder> selectParameterBinders) {
		super(sql, columnIndexes, parameterBinders);
		this.selectParameterBinders = selectParameterBinders;
	}
	
	public Map<String, ParameterBinder> getSelectParameterBinders() {
		return selectParameterBinders;
	}
}
