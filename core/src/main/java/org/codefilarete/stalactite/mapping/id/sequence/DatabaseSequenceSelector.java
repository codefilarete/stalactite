package org.codefilarete.stalactite.mapping.id.sequence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.tool.VisibleForTesting;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Accessor for database sequence value.
 * 
 * @author Guillaume Mary
 */
public class DatabaseSequenceSelector implements org.codefilarete.tool.function.Sequence<Long> {
	
	private final Sequence databaseSequence;
	private final String selectStatement;
	private final int poolSize;
	private final ConnectionProvider connectionProvider;
	private long currentValue;
	
	private boolean initialized = false;
	
	public DatabaseSequenceSelector(Sequence databaseSequence, String selectStatement, ConnectionProvider connectionProvider) {
		this.databaseSequence = databaseSequence;
		this.selectStatement = selectStatement;
		this.poolSize = preventNull(databaseSequence.getBatchSize(), 1);
		this.connectionProvider = connectionProvider;
	}
	
	public Sequence getDatabaseSequence() {
		return databaseSequence;
	}
	
	@Override
	public synchronized Long next() {
		if (!initialized) {
			currentValue = callDatabase();
			initialized = true;
			return currentValue;
		} else if (currentValue % poolSize == 0) {
			currentValue = callDatabase();
			return currentValue;
		} else {
			return ++currentValue;
		}
	}
	
	@VisibleForTesting
	long callDatabase() {
		// Connection Management :
		// - sequence are independent of transactions, though no need to invoke executeInSeparateTransaction(..)
		// - we don't close it (through try-with-resource) because it will impact caller
		//   and giveConnection() is expected to return the one "already in use"
		Connection currentConnection = connectionProvider.giveConnection();
		try (PreparedStatement ps = currentConnection.prepareStatement(selectStatement)) {
			ResultSet rs = ps.executeQuery();
			rs.next();
			// we use index access to read next value column to avoid keeping the column name in this class
			// moreover, it has no interest here since we only retrieve one column
			return rs.getLong(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
