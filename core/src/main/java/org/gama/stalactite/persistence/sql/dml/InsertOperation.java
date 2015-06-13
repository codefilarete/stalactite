package org.gama.stalactite.persistence.sql.dml;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.gama.stalactite.persistence.mapping.StatementValues;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class InsertOperation extends WriteOperation {
	
	/** Column indexes for written columns */
	private Map<Column, Map.Entry<Integer, ParameterBinder>> insertIndexes;
	
	/**
	 * 
	 * @param sql
	 * @param insertIndexes
	 */
	public InsertOperation(String sql, Map<Column, Integer> insertIndexes) {
		super(sql);
		this.insertIndexes = getBinders(insertIndexes);
	}

	public Map<Column, Map.Entry<Integer, ParameterBinder>> getInsertIndexes() {
		return insertIndexes;
	}

	@Override
	protected void applyValues(StatementValues values) throws SQLException {
		super.applyValues(values);
		applyUpsertValues(insertIndexes, values);
		addBatch();
	}
	
	@Override
	protected void prepare(Connection connection) throws SQLException {
		// TODO: constuire le PreparedStatement en fonction des clés retournées
		// http://forum.spring.io/forum/spring-projects/data/52298-get-auto-generated-keys-with-jdbctemplate
		super.prepare(connection);
	}
}
