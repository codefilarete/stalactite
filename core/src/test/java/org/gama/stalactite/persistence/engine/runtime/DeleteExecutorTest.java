package org.gama.stalactite.persistence.engine.runtime;

import java.util.Map;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.RowCountManager;
import org.gama.stalactite.persistence.engine.StaleObjectExcepion;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.stalactite.test.PairSetList.pairSetList;
import static org.mockito.Mockito.*;

class DeleteExecutorTest extends AbstractDMLExecutorTest {
	
	private final Dialect dialect = new Dialect(new JavaTypeToSqlTypeMapping()
		.with(Integer.class, "int"));
	
	private DeleteExecutor<Toto, Integer, Table> testInstance;
	
	@BeforeEach
	void setUp() {
		PersistenceConfiguration<Toto, Integer, Table> persistenceConfiguration = giveDefaultPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new DeleteExecutor<>(persistenceConfiguration.classMappingStrategy,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, Retryer.NO_RETRY, 3);
	}
	
	@Test
	void delete() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(7, 17, 23)));
		
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(1)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("delete from Toto where a = ?");
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void listenerIsCalled() {
		SQLOperationListener<Column<Table, Object>> listenerMock = mock(SQLOperationListener.class);
		testInstance.setOperationListener(listenerMock);
		
		ArgumentCaptor<Map<Column<Table, Object>, ?>> statementArgCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<SQLStatement<Column<Table, Object>>> sqlArgCaptor = ArgumentCaptor.forClass(SQLStatement.class);
		
		try {
			testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31)));
		} catch (StaleObjectExcepion e) {
			// we don't care about any existing data in the database, listener must be called, so we continue even if there are some stale objects
		}
		
		Table mappedTable = new Table("Toto");
		Column colA = mappedTable.addColumn("a", Integer.class);
		Column colB = mappedTable.addColumn("b", Integer.class);
		Column colC = mappedTable.addColumn("c", Integer.class);
		verify(listenerMock, times(2)).onValuesSet(statementArgCaptor.capture());
		assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
				Maps.asHashMap(colA, 1),
				Maps.asHashMap(colA, 2)
		));
		verify(listenerMock, times(1)).onExecute(sqlArgCaptor.capture());
		assertThat(sqlArgCaptor.getValue().getSQL()).isEqualTo("delete from Toto where a = ?");
	}
	
	@Test
	void delete_multiple() throws Exception {
		testInstance.setRowCountManager(RowCountManager.NOOP_ROW_COUNT_MANAGER);
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a = ?"));
		verify(jdbcMock.preparedStatement, times(4)).addBatch();
		verify(jdbcMock.preparedStatement, times(2)).executeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(4)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(1, 2).add(1, 3).newRow(1, 4);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById() throws Exception {
		testInstance.deleteById(Arrays.asList(new Toto(7, 17, 23)));
		
		verify(jdbcMock.preparedStatement, times(0)).addBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(1)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("delete from Toto where a in (?)");
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_multiple_lastBlockContainsOneValue() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a in (?, ?, ?)", "delete from Toto " 
				+ "where a in (?)"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(4)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(2, 2).add(3, 3).newRow(1, 4);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_multiple_lastBlockContainsMultipleValue() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a in (?, ?, ?)", "delete from Toto " 
				+ "where a in (?, ?)"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(5)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(2, 2).add(3, 3).newRow(1, 4).add(2, 5);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_multiple_lastBlockSizeIsInOperatorSize() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61), new Toto(6, 67, 71)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a in (?, ?, ?)"));
		verify(jdbcMock.preparedStatement, times(2)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(6)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.newRow(1, 1).add(2, 2).add(3, 3)
				.newRow(1, 4).add(2, 5).add(3, 6);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_composedId_multiple_lastBlockContainsOneValue() throws Exception {
		PersistenceConfiguration<Toto, Toto, Table> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		DeleteExecutor<Toto, Toto, Table>testInstance = new DeleteExecutor<>(persistenceConfiguration.classMappingStrategy,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, Retryer.NO_RETRY, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", 
				"delete from Toto where (a, b) in ((?, ?))"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(8)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 17).add(3, 2).add(4, 29).add(5, 3).add(6, 37)
				.newRow(1, 4).add(2, 43);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_composedId_multiple_lastBlockContainsMultipleValue() throws Exception {
		PersistenceConfiguration<Toto, Toto, Table> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		DeleteExecutor<Toto, Toto, Table>testInstance = new DeleteExecutor<>(persistenceConfiguration.classMappingStrategy,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, Retryer.NO_RETRY, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", 
				"delete from Toto where (a, b) in ((?, ?), (?, ?))"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(10)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 17).add(3, 2).add(4, 29).add(5, 3).add(6, 37)
				.newRow(1, 4).add(2, 43).add(3, 5).add(4, 59);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_composedId_multiple_lastBlockSizeIsInOperatorSize() throws Exception {
		PersistenceConfiguration<Toto, Toto, Table> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		DeleteExecutor<Toto, Toto, Table>testInstance = new DeleteExecutor<>(persistenceConfiguration.classMappingStrategy,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, Retryer.NO_RETRY, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61), new Toto(6, 67, 71)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))"));
		verify(jdbcMock.preparedStatement, times(2)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeUpdate();
		verify(jdbcMock.preparedStatement, times(12)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 17).add(3, 2).add(4, 29).add(5, 3).add(6, 37)
				.newRow(1, 4).add(2, 43).add(3, 5).add(4, 59).add(5, 6).add(6, 67);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
}