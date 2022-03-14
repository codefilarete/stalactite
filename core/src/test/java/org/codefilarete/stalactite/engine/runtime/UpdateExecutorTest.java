package org.codefilarete.stalactite.engine.runtime;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.PairIterator;
import org.codefilarete.reflection.AccessorByField;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.stalactite.engine.StaleStateObjectException;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener.UpdatePayload;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.tool.collection.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class UpdateExecutorTest extends AbstractDMLExecutorMockTest {
	
	private final Dialect dialect = new Dialect(new JavaTypeToSqlTypeMapping()
		.with(Integer.class, "int"));
	
	private UpdateExecutor<Toto, Integer, Table> testInstance;
	
	@BeforeEach
	void setUp() {
		PersistenceConfiguration<Toto, Integer, Table> persistenceConfiguration = giveDefaultPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new UpdateExecutor<>(persistenceConfiguration.classMappingStrategy,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, noRowCountCheckWriteOperationFactory, 3);
	}
	
	@Test
	void updateById() throws Exception {
		testInstance.updateById(asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		
		verify(jdbcMock.preparedStatement, times(4)).addBatch();
		verify(jdbcMock.preparedStatement, times(2)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(12)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("update Toto set b = ?, c = ? where a = ?");
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.newRow(1, 17).add(2, 23).add(3, 1)
				.newRow(1, 29).add(2, 31).add(3, 2)
				.newRow(1, 37).add(2, 41).add(3, 3)
				.newRow(1, 43).add(2, 53).add(3, 4);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void update_mandatoryColumn() {
		Column<Table, Object> bColumn = (Column<Table, Object>) testInstance.getMapping().getTargetTable().mapColumnsOnName().get("b");
		bColumn.setNullable(false);
		
		List<Duo<Toto, Toto>> differencesIterable = asList(new Duo<>(new Toto(1, null, 23), new Toto(1, 42, 23)));
		Iterable<UpdatePayload<Toto, Table>> payloads = UpdateListener.computePayloads(differencesIterable, true,
				testInstance.getMapping());
		
		assertThatThrownBy(() -> testInstance.updateMappedColumns(payloads))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Expected non null value for column Toto.b on instance Toto{a=1, b=null, c=23}");
	}
	
	@Test
	void listenerIsCalled() {
		SQLOperationListener<UpwhereColumn<Table>> listenerMock = mock(SQLOperationListener.class);
		testInstance.setOperationListener(listenerMock);
		
		ArgumentCaptor<Map<UpwhereColumn<Table>, ?>> statementArgCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<SQLStatement<UpwhereColumn<Table>>> sqlArgCaptor = ArgumentCaptor.forClass(SQLStatement.class);
		
		try {
			testInstance.updateById(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31)));
		} catch (StaleStateObjectException e) {
			// we don't care about any existing data in the database, listener must be called, so we continue whenever there are some stale objects
		}
		
		Table mappedTable = new Table("Toto");
		Column colA = mappedTable.addColumn("a", Integer.class);
		Column colB = mappedTable.addColumn("b", Integer.class);
		Column colC = mappedTable.addColumn("c", Integer.class);
		verify(listenerMock, times(2)).onValuesSet(statementArgCaptor.capture());
		assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
				Maps.asHashMap(new UpwhereColumn<>(colA, false), 1).add(new UpwhereColumn<>(colB, true), 17).add(new UpwhereColumn<>(colC, true),
						23),
				Maps.asHashMap(new UpwhereColumn<>(colA, false), 2).add(new UpwhereColumn<>(colB, true), 29).add(new UpwhereColumn<>(colC, true), 31)
		));
		verify(listenerMock, times(1)).onExecute(sqlArgCaptor.capture());
		assertThat(sqlArgCaptor.getValue().getSQL()).isEqualTo("update Toto set b = ?, c = ? where a = ?");
	}
	
	@Test
	<T extends Table<T>> void updateById_noColumnToUpdate() {
		T table = (T) new Table<>("SimpleEntity");
		Column<?, Long> id = table.addColumn("id", long.class).primaryKey();
		AccessorByField<SimpleEntity, Long> idAccessor = Accessors.accessorByField(SimpleEntity.class, "id");
		ClassMapping<SimpleEntity, Long, T> simpleEntityPersistenceMapping = new ClassMapping<SimpleEntity, Long, T>
				(SimpleEntity.class, table, (Map) Maps.asMap(idAccessor, id), idAccessor, new AlreadyAssignedIdentifierManager<>(long.class, c -> {}, c -> true));
		UpdateExecutor<SimpleEntity, Long, T> testInstance = new UpdateExecutor<SimpleEntity, Long, T>(
				simpleEntityPersistenceMapping, new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 4), new DMLGenerator(new ColumnBinderRegistry()), new WriteOperationFactory(), 4);
		
		assertThatCode(() -> testInstance.updateById(Arrays.asList(new SimpleEntity()))).doesNotThrowAnyException();
	}
	
	public static Object[][] testUpdate_diff_data() {
		return new Object[][] {
				// extreme case: no differences
				new Object[] { asList(new Toto(1, 1, 1), new Toto(2, 2, 2)),
						asList(new Toto(1, 1, 1), new Toto(2, 2, 2)),
						new ExpectedResult_TestUpdate_diff(0, 0, 0, Collections.EMPTY_LIST, new PairSetList<>()) },
				// case: always the same kind of modification: only "b" field
				new Object[] { asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3)),
						asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3)),
						new ExpectedResult_TestUpdate_diff(3, 1, 6,
								asList("update Toto set b = ? where a = ?"),
								new PairSetList<Integer, Integer>()
										.newRow(1, 2).add(2, 1)
										.add(1, 3).add(2, 2)
										.add(1, 4).add(2, 3)) },
				// case: always the same kind of modification: only "b" field, but batch should be called twice
				new Object[] { asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
						asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3), new Toto(4, 5, 4)),
						new ExpectedResult_TestUpdate_diff(4, 2, 8,
								asList("update Toto set b = ? where a = ?"),
								new PairSetList<Integer, Integer>()
										.newRow(1, 2).add(2, 1)
										.add(1, 3).add(2, 2)
										.add(1, 4).add(2, 3)
										.add(1, 5).add(2, 4)) },
				// case: always the same kind of modification: "b" + "c" fields
				new Object[] { asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
						asList(new Toto(1, 11, 11), new Toto(2, 22, 22), new Toto(3, 33, 33), new Toto(4, 44, 44)),
						new ExpectedResult_TestUpdate_diff(4, 2, 12,
								asList("update Toto set b = ?, c = ? where a = ?"),
								new PairSetList<Integer, Integer>()
										.newRow(1, 11).add(2, 11).add(3, 1)
										.add(1, 22).add(2, 22).add(3, 2)
										.add(1, 33).add(2, 33).add(3, 3)
										.add(1, 44).add(2, 44).add(3, 4)) },
				// more complex case: mix of modification sort, with batch updates
				new Object[] { asList(
						new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3),
						new Toto(4, 4, 4), new Toto(5, 5, 5), new Toto(6, 6, 6), new Toto(7, 7, 7)),
						asList(
								new Toto(1, 11, 1), new Toto(2, 22, 2), new Toto(3, 33, 3),
								new Toto(4, 44, 444), new Toto(5, 55, 555), new Toto(6, 66, 666), new Toto(7, 7, 7)),
						new ExpectedResult_TestUpdate_diff(6, 2, 15,
								asList("update Toto set b = ? where a = ?", "update Toto set b = ?, c = ? where a = ?"),
								new PairSetList<Integer, Integer>()
										.newRow(1, 11).add(2, 1)
										.add(1, 22).add(2, 2)
										.add(1, 33).add(2, 3)
										.add(1, 44).add(2, 444).add(3, 4)
										.add(1, 55).add(2, 555).add(3, 5)
										.add(1, 66).add(2, 666).add(3, 6)) }
		};
	}
	
	private static class ExpectedResult_TestUpdate_diff {
		private final int addBatchCallCount;
		private final int executeBatchCallCount;
		private final int setIntCallCount;
		private final List<String> updateStatements;
		private final PairSetList<Integer, Integer> statementValues;
		
		public ExpectedResult_TestUpdate_diff(int addBatchCallCount, int executeBatchCallCount, int setIntCallCount, List<String> updateStatements, PairSetList<Integer, Integer> statementValues) {
			this.addBatchCallCount = addBatchCallCount;
			this.executeBatchCallCount = executeBatchCallCount;
			this.setIntCallCount = setIntCallCount;
			this.updateStatements = updateStatements;
			this.statementValues = statementValues;
		}
	}
	
	@ParameterizedTest
	@MethodSource("testUpdate_diff_data")
	void update_diff(List<Toto> originalInstances, List<Toto> modifiedInstances, ExpectedResult_TestUpdate_diff expectedResult) throws Exception {
		// variable introduced to bypass generics problem
		UpdateExecutor<Toto, Integer, Table> localTestInstance = testInstance;
		localTestInstance.updateVariousColumns(UpdateListener.computePayloads(() -> new PairIterator<>(modifiedInstances, originalInstances),
				false, localTestInstance.getMapping()));
		
		verify(jdbcMock.preparedStatement, times(expectedResult.addBatchCallCount)).addBatch();
		verify(jdbcMock.preparedStatement, times(expectedResult.executeBatchCallCount)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(expectedResult.setIntCallCount)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(expectedResult.updateStatements);
		assertCapturedPairsEqual(jdbcMock, expectedResult.statementValues);
	}
	
	@Test
	void update_diff_allColumns() throws Exception {
		List<Toto> originalInstances = asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, -1, -2));
		List<Toto> modifiedInstances = asList(
				new Toto(1, 17, 123), new Toto(2, 129, 31), new Toto(3, 137, 141),
				new Toto(4, 143, 153), new Toto(5, -1, -2));
		// variable introduced to bypass generics problem
		UpdateExecutor<Toto, Integer, Table> localTestInstance = testInstance;
		localTestInstance.updateMappedColumns(UpdateListener.computePayloads(() -> new PairIterator<>(modifiedInstances, originalInstances),
				true, localTestInstance.getMapping()));
		
		verify(jdbcMock.preparedStatement, times(4)).addBatch();
		verify(jdbcMock.preparedStatement, times(2)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(12)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(asList("update Toto set b = ?, c = ? where a = ?"));
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.newRow(1, 17).add(2, 123).add(3, 1)
				.newRow(1, 129).add(2, 31).add(3, 2)
				.newRow(1, 137).add(2, 141).add(3, 3)
				.newRow(1, 143).add(2, 153).add(3, 4);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	<T extends Table<T>> void update_noColumnToUpdate() {
		T table = (T) new Table<>("SimpleEntity");
		Column<?, Long> id = table.addColumn("id", long.class).primaryKey();
		AccessorByField<SimpleEntity, Long> idAccessor = Accessors.accessorByField(SimpleEntity.class, "id");
		ClassMapping<SimpleEntity, Long, T> simpleEntityPersistenceMapping = new ClassMapping<SimpleEntity, Long, T>
				(SimpleEntity.class, table, (Map) Maps.asMap(idAccessor, id), idAccessor, new AlreadyAssignedIdentifierManager<>(long.class, c -> {}, c -> false));
		UpdateExecutor<SimpleEntity, Long, T> testInstance = new UpdateExecutor<>(
				simpleEntityPersistenceMapping, new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 4), new DMLGenerator(new ColumnBinderRegistry()), new WriteOperationFactory(), 4);
		
		
		Iterable<UpdatePayload<SimpleEntity, T>> updatePayloads = UpdateListener.computePayloads(Arrays.asList(new Duo<>(new SimpleEntity(), 
				new SimpleEntity())), true, testInstance.getMapping());
		assertThatCode(() -> testInstance.updateMappedColumns(updatePayloads)).doesNotThrowAnyException();
	}
	
	private static class SimpleEntity {
		
		private long id;
		
		private SimpleEntity() {
		}
		
	}
}