package org.stalactite.persistence.sql.dml;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

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
	
	public void setValue(@Nonnull Column column, Object value) throws SQLException {
		set(insertIndexes, column, value);
	}
	
	protected void applyValues(PersistentValues values) throws SQLException {
		for (Entry<Column, Object> colToValues : values.getUpsertValues().entrySet()) {
			setValue(colToValues.getKey(), colToValues.getValue());
		}
		addBatch();
	}
	
	@Override
	protected void prepare(Connection connection) throws SQLException {
		// TODO: constuire le PreparedStatement en fonction des clés retournées
		// http://forum.spring.io/forum/spring-projects/data/52298-get-auto-generated-keys-with-jdbctemplate
		super.prepare(connection);
	}
}
