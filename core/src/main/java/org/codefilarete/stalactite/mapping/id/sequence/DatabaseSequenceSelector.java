package org.codefilarete.stalactite.mapping.id.sequence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.StringParamedSQL;
import org.codefilarete.tool.VisibleForTesting;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Accessor for database sequence value.
 * 
 * @author Guillaume Mary
 */
public class DatabaseSequenceSelector implements org.codefilarete.tool.function.Sequence<Long> {
	
	private final Sequence databaseSequence;
	private final int poolSize;
	private final ReadOperationFactory readOperationFactory;
	private final ConnectionProvider connectionProvider;
	private final InternalState internalState = new InternalState();
	private final StringParamedSQL sqlOrder;
	
	public DatabaseSequenceSelector(Sequence databaseSequence, String selectStatement, ReadOperationFactory readOperationFactory, ConnectionProvider connectionProvider) {
		this.databaseSequence = databaseSequence;
		this.poolSize = preventNull(databaseSequence.getBatchSize(), 1);
		this.readOperationFactory = readOperationFactory;
		this.connectionProvider = connectionProvider;
		this.sqlOrder = new StringParamedSQL(selectStatement, Collections.EMPTY_MAP);
	}
	
	public Sequence getDatabaseSequence() {
		return databaseSequence;
	}
	
	@Override
	public synchronized Long next() {
		if (!internalState.initialized) {
			internalState.currentValue = callDatabase();
			internalState.initialized = true;
			return internalState.currentValue;
		} else if (internalState.currentValue % poolSize == 0) {
			internalState.currentValue = callDatabase();
			return internalState.currentValue;
		} else {
			return ++internalState.currentValue;
		}
	}
	
	@VisibleForTesting
	long callDatabase() {
		try (ReadOperation<String> readOperation = readOperationFactory.createInstance(sqlOrder, connectionProvider, 1)) {
			ResultSet rs = readOperation.execute();
			rs.next();
			// we use index access to read next value column to avoid keeping the column name in this class
			// moreover, it has no interest here since we only retrieve one column
			return rs.getLong(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	private static class InternalState {
		private long currentValue;
		private boolean initialized = false;
	}
}
