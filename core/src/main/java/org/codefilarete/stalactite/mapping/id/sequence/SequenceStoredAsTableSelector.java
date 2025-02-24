package org.codefilarete.stalactite.mapping.id.sequence;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.collection.Maps;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.codefilarete.tool.collection.Arrays.asList;

/**
 * Reader for a table that acts as a sequence.
 * The table contains only one line, which is updated when the sequence is asked for its next value.
 * The updates follow the sequence increment size.
 * 
 * @author Guillaume Mary
 */
public class SequenceStoredAsTableSelector implements org.codefilarete.tool.function.Sequence<Long> {
	
	private final SequenceAsTable sequenceTable;
	private final int initialValue;
	private final int batchSize;
	private final ConnectionProvider connectionProvider;
	
	private final PreparedSQL selectSequenceStatement;
	private final PreparedSQL insertSequenceStatement;
	private final PreparedSQL updateSequenceStatement;
	private final ReadOperationFactory readOperationFactory;
	private final WriteOperationFactory writeOperationFactory;
	
	/**
	 * Normal constructor
	 * 
	 * @param schema optional schema to store the table in
	 * @param tableName name of the sequence, used for storing-table name
	 * @param initialValue initial sequence value
	 * @param batchSize sequence increment
	 * @param dmlGenerator DML generator for select, update and insert statements
	 * @param readOperationFactory select operation factory
	 * @param writeOperationFactory update and insert operations factory
	 * @param connectionProvider statements connection provider
	 */
	public SequenceStoredAsTableSelector(@Nullable Schema schema,
										 String tableName,
										 int initialValue,
										 int batchSize,
										 DMLGenerator dmlGenerator,
										 ReadOperationFactory readOperationFactory,
										 WriteOperationFactory writeOperationFactory,
										 ConnectionProvider connectionProvider) {
		this.readOperationFactory = readOperationFactory;
		this.writeOperationFactory = writeOperationFactory;
		this.sequenceTable = new SequenceAsTable(schema, tableName);
		this.initialValue = initialValue;
		this.batchSize = batchSize;
		this.connectionProvider = connectionProvider;
		
		this.selectSequenceStatement = new PreparedSQL(dmlGenerator
				.buildSelect(sequenceTable, asList(sequenceTable.nextVal), emptyList()).getSQL(), emptyMap());
		Map<Integer, ParameterBinder<Long>> writeBinders = Maps.asMap(1, DefaultParameterBinders.LONG_BINDER);
		this.insertSequenceStatement = new PreparedSQL(dmlGenerator
				.buildInsert(asList(sequenceTable.nextVal)).getSQL(), writeBinders);
		this.updateSequenceStatement = new PreparedSQL(dmlGenerator
				.buildUpdate(asList(sequenceTable.nextVal), emptyList()).getSQL(), writeBinders);
	}
	
	public Table getSequenceTable() {
		return sequenceTable;
	}
	
	/**
	 * Gives current sequence value, based on the lonely line stored in the table.
	 * Write the new value after reading (computed with increment value).
	 * @return current sequence value
	 */
	@Override
	public synchronized Long next() {
		long result;
		boolean hasData;
		try (ReadOperation<Integer> readOperation = readOperationFactory.createInstance(selectSequenceStatement, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			hasData = resultSet.next();
			if (hasData) {
				result = resultSet.getLong(1);
			} else {
				result = initialValue;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		WriteOperation<Integer> writeOperation;
		long valueToWrite = result + batchSize;
		if (hasData) {
			writeOperation = writeOperationFactory.createInstance(updateSequenceStatement, connectionProvider);
		} else {
			writeOperation = writeOperationFactory.createInstance(insertSequenceStatement, connectionProvider);
		}
		try (WriteOperation<Integer> ignored = writeOperation) {
			writeOperation.setValue(1, valueToWrite);
			writeOperation.execute();
		}
		return result;
	}
	
	/**
	 * Particular {@link Table} which stores sequence value
	 * @author Guillaume Mary
	 */
	private static class SequenceAsTable extends Table<SequenceAsTable> {
		
		private final Column<SequenceAsTable, Long> nextVal = addColumn("nextVal", long.class).primaryKey();
		
		public SequenceAsTable(@Nullable Schema schema, String name) {
			super(schema, name);
		}
	}
}