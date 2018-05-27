package org.gama.stalactite.persistence.sql.dml;

import java.sql.PreparedStatement;
import java.util.Map;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.PreparedStatementWriter;
import org.gama.sql.binder.PreparedStatementWriterIndex;
import org.gama.sql.dml.SQLStatement;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Statement dedicated to updates: a parameter can be in the where clause and the update one with different values.
 * So we must distinct those parameters. This is done with {@link UpwhereColumn}.
 * 
 * @author Guillaume Mary
 */
public class PreparedUpdate<T extends Table> extends SQLStatement<UpwhereColumn<T>> {
	
	private final String sql;
	private final Map<UpwhereColumn<T>, Integer> columnIndexes;
	
	public PreparedUpdate(String sql, Map<UpwhereColumn<T>, Integer> columnIndexes, Map<UpwhereColumn<T>, ParameterBinder> parameterBinders) {
		super(parameterBinders);
		this.sql = sql;
		this.columnIndexes = columnIndexes;
	}
	
	public PreparedUpdate(String sql, Map<UpwhereColumn<T>, Integer> columnIndexes, PreparedStatementWriterIndex<UpwhereColumn<T>, ? extends PreparedStatementWriter> parameterBinderProvider) {
		super(parameterBinderProvider);
		this.sql = sql;
		this.columnIndexes = columnIndexes;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	@Override
	protected void doApplyValue(UpwhereColumn column, Object value, PreparedStatement statement) {
		PreparedStatementWriter<Object> parameterBinder = getParameterBinder(column);
		if (parameterBinder == null) {
			throw new BindingException("Can't find a " + ParameterBinder.class.getName() + " for column " + column.getColumn().getAbsoluteName()
					+ " of value " + value + " on sql : " + getSQL());
		}
		doApplyValue(getIndex(column), value, parameterBinder, statement);
	}
	
	public int getIndex(UpwhereColumn column) {
		return columnIndexes.get(column);
	}
	
}
