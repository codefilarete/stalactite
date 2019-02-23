package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.gama.stalactite.test.PairSetList.pairSetList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
		assertEquals("select a, b, c from Toto where a in (?)", dataSet.statementArgCaptor.getValue());
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
		assertEquals(Arrays.asList(
				Maps.asHashMap(colA, Arrays.asList(1, 2))
		), statementArgCaptor.getAllValues());
		verify(listenerMock, times(1)).onExecute(sqlArgCaptor.capture());
		assertEquals("select a, b, c from Toto where a in (?, ?)", sqlArgCaptor.getValue().getSQL());
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
		assertEquals(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)", "select a, b, c from Toto where a in (?)"), dataSet.statementArgCaptor.getAllValues());
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
		assertEquals(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)", "select a, b, c from Toto where a in (?, ?)"), dataSet.statementArgCaptor.getAllValues());
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
		assertEquals(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)"), dataSet.statementArgCaptor.getAllValues());
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
		assertEquals(Arrays.asList("select a, b, c from Toto where a in (?, ?)"), dataSet.statementArgCaptor.getAllValues());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 11).add(2, 13);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testSelect_multiple_emptyArgument() throws SQLException {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(dataSet.preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		List<Toto> result = testInstance.select(Arrays.asList());
		
		assertTrue(result.isEmpty());
		
		// No queries
		verify(dataSet.preparedStatement, times(0)).executeQuery();
		verify(dataSet.preparedStatement, times(0)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertTrue(dataSet.statementArgCaptor.getAllValues().isEmpty());
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
		assertEquals(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", "select a, b, c from Toto where (a, b) in ((?, ?))"), dataSet.statementArgCaptor.getAllValues());
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
		assertEquals(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", "select a, b, c from Toto where (a, b) in ((?, ?), (?, ?))"), dataSet.statementArgCaptor.getAllValues());
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
		assertEquals(Arrays.asList("select a, b, c from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))"), dataSet.statementArgCaptor.getAllValues());
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
		DDLDeployer ddlDeployer = new DDLDeployer(dataSet.dialect.getDdlSchemaGenerator(), dataSet.transactionManager);
		ddlDeployer.getDdlSchemaGenerator().addTables(dataSet.persistenceConfiguration.targetTable);
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
		assertEquals(expectedResult.toString(), result.toString());
	}
	
	@ParameterizedTest
	@MethodSource("datasources")
	public void testSelect_realLife_composedId_idIsASatellite(DataSource dataSource) throws SQLException {
		DataSetWithComposedId2 dataSet = new DataSetWithComposedId2();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		
		dataSet.transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dataSet.dialect.getDdlSchemaGenerator(), dataSet.transactionManager);
		ddlDeployer.getDdlSchemaGenerator().addTables(dataSet.persistenceConfiguration.targetTable);
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
		assertEquals(expectedResult, new HashSet<>(result));
	}
	
	@ParameterizedTest
	@MethodSource("datasources")
	public void testSelect_realLife(DataSource dataSource) throws SQLException {
		dataSet.transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dataSet.dialect.getDdlSchemaGenerator(), dataSet.transactionManager);
		ddlDeployer.getDdlSchemaGenerator().addTables(dataSet.persistenceConfiguration.targetTable);
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
		assertEquals(1, (Object) t.a);
		assertEquals(10, (Object) t.b);
		assertEquals(100, (Object) t.c);
		
		// test with 3 ids
		totos = testInstance.select(Arrays.asList(2, 3, 4));
		List<Toto> expectedResult = Arrays.asList(
				new Toto(2, 20, 200),
				new Toto(3, 30, 300),
				new Toto(4, 40, 400));
		System.out.println(totos);
		assertEquals(expectedResult.toString(), totos.toString());
	}
	
}