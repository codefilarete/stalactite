package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.SelectExecutor.InternalExecutor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSelect;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.codefilarete.stalactite.test.PairSetList.pairSetList;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class SelectExecutorTest<T extends Table<T>> extends AbstractDMLExecutorMockTest {
	
	private final Dialect dialect = new DefaultDialect(new JavaTypeToSqlTypeMapping()
		.with(Integer.class, "int"));
	
	private SelectExecutor<Toto, Integer, T> testInstance;
	
	@BeforeEach
	public void setUp() throws SQLException {
		PersistenceConfiguration<Toto, Integer, T> persistenceConfiguration = giveDefaultPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		testInstance = new SelectExecutor<>(persistenceConfiguration.classMappingStrategy, jdbcMock.transactionManager, dmlGenerator, 3);
	}
	
	@Test
	void select_one() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(7));
		
		verify(jdbcMock.preparedStatement).executeQuery();
		verify(jdbcMock.preparedStatement).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("select a, b, c from Toto where a in (?)");
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void listenerIsCalled() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		SQLOperationListener<Column<T, ?>> listenerMock = mock(SQLOperationListener.class);
		testInstance.setOperationListener(listenerMock);
		
		ArgumentCaptor<Map<Column<T, ?>, ?>> statementArgCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<SQLStatement<Column<T, ?>>> sqlArgCaptor = ArgumentCaptor.forClass(SQLStatement.class);
		
		testInstance.select(Arrays.asList(1, 2));
		
		Table mappedTable = new Table("Toto");
		Column colA = mappedTable.addColumn("a", Integer.class);
		Column colB = mappedTable.addColumn("b", Integer.class);
		Column colC = mappedTable.addColumn("c", Integer.class);
		verify(listenerMock, times(1)).onValuesSet(statementArgCaptor.capture());
		ExtendedMapAssert.assertThatMap((Map<Column, List<Integer>>) (Map) statementArgCaptor.getAllValues().get(0))
				// since Query contains columns copies we can't compare them through equals() (and since Column doesn't implement equals()/hashCode()
				.usingElementPredicate((entry1, entry2) -> entry1.getKey().getAbsoluteName().equals(entry2.getKey().getAbsoluteName())
						&& entry1.getValue().equals(entry2.getValue()))
				.containsExactlyInAnyOrder(
						entry(colA, Arrays.asList(1, 2))
				);
		verify(listenerMock, times(1)).onExecute(sqlArgCaptor.capture());
		assertThat(sqlArgCaptor.getValue().getSQL()).isEqualTo("select a, b, c from Toto where a in (?, ?)");
	}
	
	@Test
	void select_multiple_lastBlockContainsOneValue() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13, 17, 23));
		
		// two queries because in operator is bounded to 3 values
		verify(jdbcMock.preparedStatement, times(2)).executeQuery();
		verify(jdbcMock.preparedStatement, times(4)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)", "select a, b," 
				+ " c from Toto where a in (?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13).add(3, 17).newRow(1, 23);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void select_multiple_lastBlockContainsMultipleValue() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13, 17, 23, 29));
		
		// two queries because in operator is bounded to 3 values
		verify(jdbcMock.preparedStatement, times(2)).executeQuery();
		verify(jdbcMock.preparedStatement, times(5)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)", "select a, b," 
				+ " c from Toto where a in (?, ?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13).add(3, 17).newRow(1, 23).add(2, 29);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void select_multiple_lastBlockSizeIsInOperatorSize() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13, 17, 23, 29, 31));
		
		// two queries because in operator is bounded to 3 values
		verify(jdbcMock.preparedStatement, times(2)).executeQuery();
		verify(jdbcMock.preparedStatement, times(6)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13).add(3, 17).newRow(1, 23).add(2, 29).add(3, 31);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void select_multiple_argumentWithOneBlock() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13));
		
		// one query because in operator is bounded to 3 values
		verify(jdbcMock.preparedStatement, times(1)).executeQuery();
		verify(jdbcMock.preparedStatement, times(2)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void select_multiple_emptyArgument() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		Set<Toto> result = testInstance.select(Arrays.asList());
		
		assertThat(result.isEmpty()).isTrue();
		
		// No queries
		verify(jdbcMock.preparedStatement, times(0)).executeQuery();
		verify(jdbcMock.preparedStatement, times(0)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues().isEmpty()).isTrue();
	}
	
	@Test
	void select_multiple_composedId_lastBlockContainsOneValue() throws SQLException {
		PersistenceConfiguration<Toto, Toto, T> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		SelectExecutor<Toto, Toto, T> testInstance = new SelectExecutor<>(persistenceConfiguration.classMappingStrategy, jdbcMock.transactionManager, dmlGenerator, 3);
		
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(new Toto(1, 11, 111), new Toto(2, 13, 222), new Toto(3, 17, 333), new Toto(4, 23, 444)));
		
		// two queries because in operator is bounded to 3 values
		verify(jdbcMock.preparedStatement, times(2)).executeQuery();
		verify(jdbcMock.preparedStatement, times(8)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?," 
				+ " ?))", "select a, b, c from Toto where (a, b) in ((?, ?))"));
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 11).add(3, 2).add(4, 13).add(5, 3).add(6, 17)
				.newRow(1, 4).add(2, 23);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void select_multiple_composedId_lastBlockContainsMultipleValue() throws SQLException {
		PersistenceConfiguration<Toto, Toto, T> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		SelectExecutor<Toto, Toto, T> testInstance = new SelectExecutor<>(persistenceConfiguration.classMappingStrategy, jdbcMock.transactionManager, dmlGenerator, 3);
		
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(new Toto(1, 11, 111), new Toto(2, 13, 222), new Toto(3, 17, 333), new Toto(4, 23, 444), new Toto(5, 29, 555)));
		
		// two queries because in operator is bounded to 3 values
		verify(jdbcMock.preparedStatement, times(2)).executeQuery();
		verify(jdbcMock.preparedStatement, times(10)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?," 
				+ " ?))", "select a, b, c from Toto where (a, b) in ((?, ?), (?, ?))"));
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 11).add(3, 2).add(4, 13).add(5, 3).add(6, 17)
				.newRow(1, 4).add(2, 23).add(3, 5).add(4, 29);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void select_multiple_composedId_lastBlockSizeIsInOperatorSize() throws SQLException {
		PersistenceConfiguration<Toto, Toto, T> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		SelectExecutor<Toto, Toto, T> testInstance = new SelectExecutor<>(persistenceConfiguration.classMappingStrategy, jdbcMock.transactionManager, dmlGenerator, 3);
		
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(jdbcMock.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(new Toto(1, 11, 111), new Toto(2, 13, 222), new Toto(3, 17, 333),
				new Toto(4, 23, 444), new Toto(5, 29, 555), new Toto(6, 31, 666)));
		
		// two queries because in operator is bounded to 3 values
		verify(jdbcMock.preparedStatement, times(2)).executeQuery();
		verify(jdbcMock.preparedStatement, times(12)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))"));
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 11).add(3, 2).add(4, 13).add(5, 3).add(6, 17)
				.newRow(1, 4).add(2, 23).add(3, 5).add(4, 29).add(5, 6).add(6, 31);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void internalExecutor_execute() {
		T targetTable = (T) new Table("Toto");
		Column id = targetTable.addColumn("id", long.class).primaryKey();
		
		ClassMapping<Toto, Integer, T> mappingStrategyMock = mock(ClassMapping.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(targetTable);
		// the selected columns are plugged on the table ones
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> targetTable.getColumns());
		
		// mocking to prevent NPE from EntityMappingStrategyTreeSelectExecutor constructor
		IdMapping idMappingMock = mock(IdMapping.class);
		when(mappingStrategyMock.getIdMapping()).thenReturn(idMappingMock);
		
		// mocking to provide entity values
		IdentifierAssembler identifierAssemblerMock = mock(IdentifierAssembler.class);
		when(idMappingMock.getIdentifierAssembler()).thenReturn(identifierAssemblerMock);
		Map<Column<Table, Object>, Object> idValuesPerEntity = Maps.asMap(id, Arrays.asList(10, 20));
		when(identifierAssemblerMock.getColumnValues(anyList())).thenReturn(idValuesPerEntity);
		
		// mocking ResultSet transformation because we don't care about it in this test
		ReadOperation readOperationMock = mock(ReadOperation.class);
		when(readOperationMock.execute()).thenReturn(new InMemoryResultSet(Collections.emptyList()));
		when(readOperationMock.getSqlStatement()).thenReturn(new ColumnParameterizedSelect("", new HashMap<>(), new HashMap<>(), new HashMap<>()));
		
		// we're going to check if values are correctly passed to the underlying ReadOperation
		ArgumentCaptor<Map> capturedValues = ArgumentCaptor.forClass(Map.class);
		InternalExecutor<Toto, Integer, T> testInstance = new InternalExecutor<>(mappingStrategyMock);
		testInstance.execute(readOperationMock, Arrays.asList(1, 2));
		
		verify(readOperationMock).setValues(capturedValues.capture());
		assertThat(capturedValues.getValue()).isEqualTo(Maps.asMap(id, Arrays.asList(10, 20)));
	}
	
}