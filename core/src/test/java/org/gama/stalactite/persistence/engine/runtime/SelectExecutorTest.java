package org.gama.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.id.assembly.IdentifierAssembler;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.ColumnParameterizedSelect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.gama.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.stalactite.test.PairSetList.pairSetList;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class SelectExecutorTest extends AbstractDMLExecutorTest {
	
	private DataSet dataSet;
	
	private SelectExecutor<Toto, Integer, Table> testInstance;
	
	@BeforeEach
	public void setUp() throws SQLException {
		dataSet = new DataSet();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new SelectExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, 3);
	}
	
	@Test
	public void testSelect_one() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(7));
		
		verify(dataSet.preparedStatement).executeQuery();
		verify(dataSet.preparedStatement).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getValue()).isEqualTo("select a, b, c from Toto where a in (?)");
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void listenerIsCalled() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		SQLOperationListener<Column<Table, Object>> listenerMock = mock(SQLOperationListener.class);
		testInstance.setOperationListener(listenerMock);
		
		ArgumentCaptor<Map<Column<Table, Object>, ?>> statementArgCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<SQLStatement<Column<Table, Object>>> sqlArgCaptor = ArgumentCaptor.forClass(SQLStatement.class);
		
		testInstance.select(Arrays.asList(1, 2));
		
		Table mappedTable = new Table("Toto");
		Column colA = mappedTable.addColumn("a", Integer.class);
		Column colB = mappedTable.addColumn("b", Integer.class);
		Column colC = mappedTable.addColumn("c", Integer.class);
		verify(listenerMock, times(1)).onValuesSet(statementArgCaptor.capture());
		assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
				Maps.asHashMap(colA, Arrays.asList(1, 2))
		));
		verify(listenerMock, times(1)).onExecute(sqlArgCaptor.capture());
		assertThat(sqlArgCaptor.getValue().getSQL()).isEqualTo("select a, b, c from Toto where a in (?, ?)");
	}
	
	@Test
	public void testSelect_multiple_lastBlockContainsOneValue() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13, 17, 23));
		
		// two queries because in operator is bounded to 3 values
		verify(dataSet.preparedStatement, times(2)).executeQuery();
		verify(dataSet.preparedStatement, times(4)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)", "select a, b," 
				+ " c from Toto where a in (?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13).add(3, 17).newRow(1, 23);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testSelect_multiple_lastBlockContainsMultipleValue() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13, 17, 23, 29));
		
		// two queries because in operator is bounded to 3 values
		verify(dataSet.preparedStatement, times(2)).executeQuery();
		verify(dataSet.preparedStatement, times(5)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)", "select a, b," 
				+ " c from Toto where a in (?, ?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13).add(3, 17).newRow(1, 23).add(2, 29);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testSelect_multiple_lastBlockSizeIsInOperatorSize() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13, 17, 23, 29, 31));
		
		// two queries because in operator is bounded to 3 values
		verify(dataSet.preparedStatement, times(2)).executeQuery();
		verify(dataSet.preparedStatement, times(6)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13).add(3, 17).newRow(1, 23).add(2, 29).add(3, 31);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testSelect_multiple_argumentWithOneBlock() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(11, 13));
		
		// one query because in operator is bounded to 3 values
		verify(dataSet.preparedStatement, times(1)).executeQuery();
		verify(dataSet.preparedStatement, times(2)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where a in (?, ?)"));
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testSelect_multiple_emptyArgument() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		List<Toto> result = testInstance.select(Arrays.asList());
		
		assertThat(result.isEmpty()).isTrue();
		
		// No queries
		verify(dataSet.preparedStatement, times(0)).executeQuery();
		verify(dataSet.preparedStatement, times(0)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues().isEmpty()).isTrue();
	}
	
	@Test
	public void testSelect_multiple_composedId_lastBlockContainsOneValue() throws SQLException {
		DataSetWithComposedId dataSet = new DataSetWithComposedId();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		SelectExecutor<Toto, Toto, Table> testInstance = new SelectExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, 3);
		
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(new Toto(1, 11, 111), new Toto(2, 13, 222), new Toto(3, 17, 333), new Toto(4, 23, 444)));
		
		// two queries because in operator is bounded to 3 values
		verify(dataSet.preparedStatement, times(2)).executeQuery();
		verify(dataSet.preparedStatement, times(8)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?," 
				+ " ?))", "select a, b, c from Toto where (a, b) in ((?, ?))"));
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 11).add(3, 2).add(4, 13).add(5, 3).add(6, 17)
				.newRow(1, 4).add(2, 23);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testSelect_multiple_composedId_lastBlockContainsMultipleValue() throws SQLException {
		DataSetWithComposedId dataSet = new DataSetWithComposedId();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		SelectExecutor<Toto, Toto, Table> testInstance = new SelectExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, 3);
		
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(new Toto(1, 11, 111), new Toto(2, 13, 222), new Toto(3, 17, 333), new Toto(4, 23, 444), new Toto(5, 29, 555)));
		
		// two queries because in operator is bounded to 3 values
		verify(dataSet.preparedStatement, times(2)).executeQuery();
		verify(dataSet.preparedStatement, times(10)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?," 
				+ " ?))", "select a, b, c from Toto where (a, b) in ((?, ?), (?, ?))"));
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 11).add(3, 2).add(4, 13).add(5, 3).add(6, 17)
				.newRow(1, 4).add(2, 23).add(3, 5).add(4, 29);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testSelect_multiple_composedId_lastBlockSizeIsInOperatorSize() throws SQLException {
		DataSetWithComposedId dataSet = new DataSetWithComposedId();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		SelectExecutor<Toto, Toto, Table> testInstance = new SelectExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, 3);
		
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList(new Toto(1, 11, 111), new Toto(2, 13, 222), new Toto(3, 17, 333),
				new Toto(4, 23, 444), new Toto(5, 29, 555), new Toto(6, 31, 666)));
		
		// two queries because in operator is bounded to 3 values
		verify(dataSet.preparedStatement, times(2)).executeQuery();
		verify(dataSet.preparedStatement, times(12)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertThat(dataSet.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))"));
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 11).add(3, 2).add(4, 13).add(5, 3).add(6, 17)
				.newRow(1, 4).add(2, 23).add(3, 5).add(4, 29).add(5, 6).add(6, 31);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	public static Object[][] datasources() {
		return new Object[][] {
				// NB: Derby can't be tested because it doesn't support "tupled in"
				new Object[] { new HSQLDBInMemoryDataSource() },
				new Object[] { new MariaDBEmbeddableDataSource(3406) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("datasources")
	public void testSelect_realLife_composedId_idIsItSelf(DataSource dataSource) throws SQLException {
		DataSetWithComposedId dataSet = new DataSetWithComposedId();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		
		dataSet.transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dataSet.dialect.getJavaTypeToSqlTypeMapping(), dataSet.transactionManager);
		ddlDeployer.getDdlGenerator().addTables(dataSet.persistenceConfiguration.targetTable);
		ddlDeployer.deployDDL();
		Connection connection = dataSource.getConnection();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		connection.commit();
		
		SelectExecutor<Toto, Toto, Table> testInstance = new SelectExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, 3);
		List<Toto> result = testInstance.select(Arrays.asList(new Toto(1, 10, null), new Toto(2, 20, null), new Toto(3, 30, null), new Toto(4, 40, null)));
		List<Toto> expectedResult = Arrays.asList(
				new Toto(1, 10, 100),
				new Toto(2, 20, 200),
				new Toto(3, 30, 300),
				new Toto(4, 40, 400));
		assertThat(result.toString()).isEqualTo(expectedResult.toString());
	}
	
	@ParameterizedTest
	@MethodSource("datasources")
	public void testSelect_realLife_composedId_idIsASatellite(DataSource dataSource) throws SQLException {
		DataSetWithComposedId2 dataSet = new DataSetWithComposedId2();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		
		dataSet.transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dataSet.dialect.getJavaTypeToSqlTypeMapping(), dataSet.transactionManager);
		ddlDeployer.getDdlGenerator().addTables(dataSet.persistenceConfiguration.targetTable);
		ddlDeployer.deployDDL();
		Connection connection = dataSource.getConnection();
		connection.prepareStatement("insert into Tata(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Tata(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Tata(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Tata(a, b, c) values (4, 40, 400)").execute();
		connection.commit();
		
		SelectExecutor<Tata, ComposedId, Table> testInstance = new SelectExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, 3);
		List<Tata> result = testInstance.select(Arrays.asList(new ComposedId(1, 10), new ComposedId(2, 20), new ComposedId(3, 30), new ComposedId(4, 40)));
		Set<Tata> expectedResult = Arrays.asHashSet(
				new Tata(1, 10, 100),
				new Tata(2, 20, 200),
				new Tata(3, 30, 300),
				new Tata(4, 40, 400));
		assertThat(new HashSet<>(result)).isEqualTo(expectedResult);
	}
	
	@ParameterizedTest
	@MethodSource("datasources")
	public void testSelect_realLife(DataSource dataSource) throws SQLException {
		dataSet.transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dataSet.dialect.getJavaTypeToSqlTypeMapping(), dataSet.transactionManager);
		ddlDeployer.getDdlGenerator().addTables(dataSet.persistenceConfiguration.targetTable);
		ddlDeployer.deployDDL();
		Connection connection = dataSource.getConnection();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		connection.commit();
		
		// test with 1 id
		List<Toto> totos = testInstance.select(Arrays.asList(1));
		Toto t = Iterables.first(totos);
		assertThat((Object) t.a).isEqualTo(1);
		assertThat((Object) t.b).isEqualTo(10);
		assertThat((Object) t.c).isEqualTo(100);
		
		// test with 3 ids
		totos = testInstance.select(Arrays.asList(2, 3, 4));
		List<Toto> expectedResult = Arrays.asList(
				new Toto(2, 20, 200),
				new Toto(3, 30, 300),
				new Toto(4, 40, 400));
		assertThat(totos.toString()).isEqualTo(expectedResult.toString());
	}
	
	@Test
	public void testExecute() {
		Table targetTable = new Table("Toto");
		Column id = targetTable.addColumn("id", long.class).primaryKey();
		
		ClassMappingStrategy mappingStrategyMock = mock(ClassMappingStrategy.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(targetTable);
		// the selected columns are plugged on the table ones
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> targetTable.getColumns());
		
		// mocking to prevent NPE from EntityMappingStrategyTreeSelectExecutor constructor
		IdMappingStrategy idMappingStrategyMock = mock(IdMappingStrategy.class);
		when(mappingStrategyMock.getIdMappingStrategy()).thenReturn(idMappingStrategyMock);
		
		// mocking to provide entity values
		IdentifierAssembler identifierAssemblerMock = mock(IdentifierAssembler.class);
		when(idMappingStrategyMock.getIdentifierAssembler()).thenReturn(identifierAssemblerMock);
		Map<Column<Table, Object>, Object> idValuesPerEntity = Maps.asMap(id, Arrays.asList(10, 20));
		when(identifierAssemblerMock.getColumnValues(anyList())).thenReturn(idValuesPerEntity);
		
		// mocking ResultSet transformation because we don't care about it in this test
		ReadOperation readOperationMock = mock(ReadOperation.class);
		when(readOperationMock.execute()).thenReturn(new InMemoryResultSet(Collections.emptyList()));
		when(readOperationMock.getSqlStatement()).thenReturn(new ColumnParameterizedSelect("", new HashMap<>(), new HashMap<>(), new HashMap<>()));
		
		// we're going to check if values are correctly passed to the underlying ReadOperation
		SelectExecutor<Toto, Integer, Table> testInstance = new SelectExecutor<>(mappingStrategyMock, mock(ConnectionProvider.class), new Dialect().getDmlGenerator(), 3);
		ArgumentCaptor<Map> capturedValues = ArgumentCaptor.forClass(Map.class);
		testInstance.new InternalExecutor().execute(readOperationMock, Arrays.asList(1, 2));
		
		verify(readOperationMock).setValues(capturedValues.capture());
		assertThat(capturedValues.getValue()).isEqualTo(Maps.asMap(id, Arrays.asList(10, 20)));
	}
	
}