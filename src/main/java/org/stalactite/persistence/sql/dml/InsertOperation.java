package org.stalactite.persistence.sql.dml;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author mary
 */
public class InsertOperation extends WriteOperation {
	
	/** Column indexes for written columns */
	private Map<Column, Integer> insertIndexes;
	
	/**
	 * 
	 * @param sql
	 * @param insertIndexes
	 */
	public InsertOperation(String sql, Map<Column, Integer> insertIndexes) {
		super(sql);
		this.insertIndexes = insertIndexes;
	}
	
	@Override
	protected void applyValues(PersistentValues values) throws SQLException {
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
