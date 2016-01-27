package org.gama.stalactite.persistence.engine;

import org.gama.lang.Reflections;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcTransactionManager;
import org.gama.stalactite.test.PairSetList;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author Guillaume Mary
 */
public class JoinTablePersisterTest {
	
	private JoinTablePersister<Toto> testInstance;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> statementArgCaptor;
	private JdbcTransactionManager transactionManager;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	private ClassMappingStrategy<Toto> totoClassMappingStrategy_ontoTable1, totoClassMappingStrategy2_ontoTable2;
	private Dialect dialect;
	private Table totoClassTable1, totoClassTable2;
	
	@BeforeTest
	public void setUp() throws SQLException {
		Field fieldId = Reflections.getField(Toto.class, "id");
		Field fieldA = Reflections.getField(Toto.class, "a");
		Field fieldB = Reflections.getField(Toto.class, "b");
		Field fieldX = Reflections.getField(Toto.class, "x");
		Field fieldY = Reflections.getField(Toto.class, "y");
		Field fieldZ = Reflections.getField(Toto.class, "z");
		
		totoClassTable1 = new Table("Toto1");
		totoClassTable1.new Column("id", fieldId.getType());
		totoClassTable1.new Column("a", fieldA.getType());
		totoClassTable1.new Column("b", fieldB.getType());
		Map<String, Table.Column> columnMap1 = totoClassTable1.mapColumnsOnName();
		columnMap1.get("id").setPrimaryKey(true);
		
		totoClassTable2 = new Table("Toto2");
		totoClassTable2.new Column("id", fieldId.getType());
		totoClassTable2.new Column("x", fieldX.getType());
		totoClassTable2.new Column("y", fieldY.getType());
		totoClassTable2.new Column("z", fieldZ.getType());
		Map<String, Table.Column> columnMap2 = totoClassTable2.mapColumnsOnName();
		columnMap2.get("id").setPrimaryKey(true);
		
		
		Map<Field, Table.Column> totoClassMapping1 = Maps.asMap(fieldId, columnMap1.get("id")).add(fieldA, columnMap1.get("a")).add(fieldB, columnMap1.get("b"));
		Map<Field, Table.Column> totoClassMapping2 = Maps.asMap(fieldId, columnMap2.get("id")).add(fieldX, columnMap2.get("x")).add(fieldY, columnMap2.get("y")).add(fieldZ, columnMap2.get("z"));
		
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		totoClassMappingStrategy_ontoTable1 = new ClassMappingStrategy<>(Toto.class, totoClassTable1,
				totoClassMapping1, fieldId, identifierGenerator);
		totoClassMappingStrategy2_ontoTable2 = new ClassMappingStrategy<>(Toto.class, totoClassTable2,
				totoClassMapping2, fieldId, AutoAssignedIdentifierGenerator.INSTANCE);
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Integer.class, "int");
		
		transactionManager = new JdbcTransactionManager(null);
		dialect = new Dialect(simpleTypeMapping);
		dialect.setInOperatorMaxSize(3);
	}
	
	@BeforeMethod
	public void setUpTest() throws SQLException {
		// reset id counter between 2 tests to keep independency between them
		identifierGenerator.reset();
		
		preparedStatement = mock(PreparedStatement.class);
		when(preparedStatement.executeBatch()).thenReturn(new int[] {1});
		
		Connection connection = mock(Connection.class);
		// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
		// weither or not it should prepare statement
		when(preparedStatement.getConnection()).thenReturn(connection);
		statementArgCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(statementArgCaptor.capture())).thenReturn(preparedStatement);
		
		valueCaptor = ArgumentCaptor.forClass(Integer.class);
		indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		DataSource dataSource = mock(DataSource.class);
		when(dataSource.getConnection()).thenReturn(connection);
		transactionManager.setDataSource(dataSource);
		testInstance = new JoinTablePersister<>(totoClassMappingStrategy_ontoTable1, dialect, transactionManager, 3);
		testInstance.addMappingStrategy(totoClassMappingStrategy2_ontoTable2, new Objects.Function<Iterable<Toto>, Iterable<Toto>>() {
			@Override
			public Iterable<Toto> apply(Iterable<Toto> totos) {
				// since we only want a replicate of totos in table2, we only need to return them
				return totos;
			}
		});
	}
	
	public void assertCapturedPairsEqual(PairSetList<Integer, Integer> expectedPairs) {
		List<Map.Entry<Integer, Integer>> obtainedPairs = PairSetList.toPairs(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		List<Set<Map.Entry<Integer, Integer>>> obtained = new ArrayList<>();
		int startIndex = 0;
		for (Set<Map.Entry<Integer, Integer>> expectedPair : expectedPairs.asList()) {
			obtained.add(new HashSet<>(obtainedPairs.subList(startIndex, startIndex += expectedPair.size())));
		}
		assertEquals(expectedPairs.asList(), obtained);
	}
	
	@Test
	public void testInsert() throws Exception {
		testInstance.insert(Arrays.asList(new Toto(17, 23, 117, 123, -117), new Toto(29, 31, 129, 131, -129), new Toto(37, 41, 137, 141, -137), new Toto(43, 53, 143, 153, -143)));
		
		verify(preparedStatement, times(8)).addBatch();
		verify(preparedStatement, times(4)).executeBatch();
		verify(preparedStatement, times(28)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList("insert into Toto1(id, a, b) values (?, ?, ?)", "insert into Toto2(id, x, y, z) values (?, ?, ?, ?)"), statementArgCaptor.getAllValues());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 1).add(2, 17).add(3, 23)
				.of(1, 2).add(2, 29).add(3, 31)
				.of(1, 3).add(2, 37).add(3, 41)
				.of(1, 4).add(2, 43).add(3, 53)
				
				.of(1, 1).add(2, 117).add(3, 123).add(4, -117)
				.of(1, 2).add(2, 129).add(3, 131).add(4, -129)
				.of(1, 3).add(2, 137).add(3, 141).add(4, -137)
				.of(1, 4).add(2, 143).add(3, 153).add(4, -143)
		;
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testUpdateRoughly() throws Exception {
		testInstance.updateRoughly(Arrays.asList(new Toto(1, 17, 23, 117, 123, -117), new Toto(2, 29, 31, 129, 131, -129), new Toto(3, 37, 41, 137, 141, -137), new Toto(4, 43, 53, 143, 153, -143)));
		
		verify(preparedStatement, times(8)).addBatch();
		verify(preparedStatement, times(4)).executeBatch();
		verify(preparedStatement, times(28)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList("update Toto1 set a = ?, b = ? where id = ?", "update Toto2 set x = ?, y = ?, z = ? where id = ?"), statementArgCaptor.getAllValues());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 17).add(2, 23).add(3, 1)
				.of(1, 29).add(2, 31).add(3, 2)
				.of(1, 37).add(2, 41).add(3, 3)
				.of(1, 43).add(2, 53).add(3, 4)
				
				.of(1, 117).add(2, 123).add(3, -117).add(4, 1)
				.of(1, 129).add(2, 131).add(3, -129).add(4, 2)
				.of(1, 137).add(2, 141).add(3, -137).add(4, 3)
				.of(1, 143).add(2, 153).add(3, -143).add(4, 4)
		;
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testDelete() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(7, 17, 23, 117, 123, -117)));
		
		verify(preparedStatement, times(2)).executeUpdate();
		verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList("delete from Toto2 where id in (?)", "delete from Toto1 where id in (?)"), statementArgCaptor.getAllValues());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 7)
				.of(1, 7);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testDelete_multiple() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23, 117, 123, -117), new Toto(2, 29, 31, 129, 131, -129), new Toto(3, 37, 41, 137, 141, -137), new Toto(4, 43, 53, 143, 153, -143)));
		// 4 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto2 where id in (?, ?, ?)", "delete from Toto2 where id in (?)", "delete from Toto1 where id in (?, ?, ?)", "delete from Toto1 where id in (?)"), statementArgCaptor.getAllValues());
		verify(preparedStatement, times(2)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(2)).executeUpdate();
		verify(preparedStatement, times(8)).setInt(indexCaptor.capture(), valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 1).add(2, 2).add(3, 3)
				.of(1, 4)
				.of(1, 1).add(2, 2).add(3, 3)
				.of(1, 4);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testSelect() throws Exception {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList((Serializable) 7, 13, 17, 23));
		
		verify(preparedStatement, times(2)).executeQuery();
		verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList(
				"select Toto1.id as Toto1_id, Toto1.a as Toto1_a, Toto1.b as Toto1_b, Toto2.id as Toto2_id, Toto2.x as Toto2_x, Toto2.y as Toto2_y, Toto2.z as Toto2_z" +
						" from Toto1 inner join Toto2 on Toto1.id = Toto2.id where Toto1.id in (?, ?, ?)", 
				"select Toto1.id as Toto1_id, Toto1.a as Toto1_a, Toto1.b as Toto1_b, Toto2.id as Toto2_id, Toto2.x as Toto2_x, Toto2.y as Toto2_y, Toto2.z as Toto2_z" +
						" from Toto1 inner join Toto2 on Toto1.id = Toto2.id where Toto1.id in (?)"), statementArgCaptor.getAllValues());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 7).add(2, 13).add(3, 17).add(1, 23);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	private static class Toto {
		private int id;
		private Integer a, b, x, y, z;
		
		public Toto() {
		}
		
		public Toto(int id, Integer a, Integer b, Integer x, Integer y, Integer z) {
			this.id = id;
			this.a = a;
			this.b = b;
			this.x = x;
			this.y = y;
			this.z = z;
		}
		
		public Toto(Integer a, Integer b, Integer x, Integer y, Integer z) {
			this.a = a;
			this.b = b;
			this.x = x;
			this.y = y;
			this.z = z;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", (Object) id).add("a", a).add("b", b).add("x", x).add("y", y).add("z", z)
					+ "]";
		}
	}
}