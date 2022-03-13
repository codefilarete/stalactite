package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.tool.trace.ModifiableInt;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.persistence.engine.InMemoryCounterIdentifierGenerator;
import org.codefilarete.stalactite.persistence.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.persistence.engine.listener.DeleteListener;
import org.codefilarete.stalactite.persistence.engine.listener.InsertListener;
import org.codefilarete.stalactite.persistence.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.persistence.engine.listener.UpdateListener;
import org.codefilarete.stalactite.persistence.engine.runtime.RelationalEntityPersister.RelationalExecutableEntityQuery;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater.EntityMappingStrategyAdapter;
import org.codefilarete.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.persistence.id.Identified;
import org.codefilarete.stalactite.persistence.id.Identifier;
import org.codefilarete.stalactite.persistence.id.PersistableIdentifier;
import org.codefilarete.stalactite.persistence.id.PersistedIdentifier;
import org.codefilarete.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.persistence.mapping.ClassMappingStrategy;
import org.codefilarete.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.codefilarete.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class SimpleRelationalEntityPersisterTest {
	
	private SimpleRelationalEntityPersister<Toto, Identifier<Integer>, ?> testInstance;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> statementArgCaptor;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	private ClassMappingStrategy<Toto, Identifier<Integer>, Table> totoClassMappingStrategy_ontoTable1;
	private Dialect dialect;
	private Column leftJoinColumn;
	private Column rightJoinColumn;
	private final EffectiveBatchedRowCount effectiveBatchedRowCount = new EffectiveBatchedRowCount();
	private final Holder<Long> expectedRowCountForUpdate = new Holder<>();
	private Connection connection;
	
	private static class EffectiveBatchedRowCount implements Sequence<long[]> {
		
		private Iterator<long[]> rowCounts;
		
		public void setRowCounts(List<long[]> rowCounts) {
			this.rowCounts = rowCounts.iterator();
		}
		
		@Override
		public long[] next() {
			return rowCounts.next();
		}
	}
	
	protected void initMapping() {
		Field fieldId = Reflections.getField(Toto.class, "id");
		Field fieldA = Reflections.getField(Toto.class, "a");
		Field fieldB = Reflections.getField(Toto.class, "b");
		
		Table totoClassTable1 = new Table("Toto1");
		leftJoinColumn = totoClassTable1.addColumn("id", fieldId.getType());
		totoClassTable1.addColumn("a", fieldA.getType());
		totoClassTable1.addColumn("b", fieldB.getType());
		Map<String, Column> columnMap1 = totoClassTable1.mapColumnsOnName();
		columnMap1.get("id").setPrimaryKey(true);
		
		PropertyAccessor<Toto, Identifier<Integer>> identifierAccessor = Accessors.propertyAccessor(fieldId);
		Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> totoClassMapping1 = Maps.asMap(
						(PropertyAccessor) identifierAccessor, (Column<Table, Object>) columnMap1.get("id"))
				.add(Accessors.propertyAccessor(fieldA), columnMap1.get("a"))
				.add(Accessors.propertyAccessor(fieldB), columnMap1.get("b"));
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		
		BeforeInsertIdentifierManager<Toto, Identifier<Integer>> beforeInsertIdentifierManager = new BeforeInsertIdentifierManager<>(
				new SinglePropertyIdAccessor<>(identifierAccessor),
				() -> new PersistableIdentifier<>(identifierGenerator.next()),
				(Class<Identifier<Integer>>) (Class) Identifier.class);
		totoClassMappingStrategy_ontoTable1 = new ClassMappingStrategy<>(Toto.class, totoClassTable1,
																		 totoClassMapping1, identifierAccessor, beforeInsertIdentifierManager);
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Identifier.class, "int");
		
		dialect = new Dialect(simpleTypeMapping);
		dialect.setInOperatorMaxSize(3);
		dialect.getColumnBinderRegistry().register(Identifier.class, new ParameterBinder<Identifier>() {
			@Override
			public Identifier doGet(ResultSet resultSet, String columnName) throws SQLException {
				return new PersistedIdentifier<>(resultSet.getObject(columnName));
			}
			
			@Override
			public void set(PreparedStatement statement, int valueIndex, Identifier value) throws SQLException {
				statement.setInt(valueIndex, (Integer) value.getSurrogate());
			}
		});
	}
	
	protected void initTest() throws SQLException {
		// reset id counter between 2 tests to keep independency between them
		identifierGenerator.reset();
		
		preparedStatement = mock(PreparedStatement.class);
		// we set row count else it will throw exception
		when(preparedStatement.executeLargeBatch()).thenAnswer((Answer<long[]>) invocation -> effectiveBatchedRowCount.next());
		when(preparedStatement.executeLargeUpdate()).thenAnswer((Answer<Long>) invocation -> expectedRowCountForUpdate.get());
		
		connection = mock(Connection.class);
		// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
		// weither or not it should prepare statement
		when(preparedStatement.getConnection()).thenReturn(connection);
		statementArgCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(statementArgCaptor.capture())).thenReturn(preparedStatement);
		when(connection.prepareStatement(statementArgCaptor.capture(), anyInt())).thenReturn(preparedStatement);
		
		valueCaptor = ArgumentCaptor.forClass(Integer.class);
		indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		DataSource dataSource = mock(DataSource.class);
		when(dataSource.getConnection()).thenReturn(connection);
		testInstance = new SimpleRelationalEntityPersister<>(totoClassMappingStrategy_ontoTable1, dialect,
															 new ConnectionConfigurationSupport(new CurrentThreadConnectionProvider(dataSource), 3));
	}
	
	void assertCapturedPairsEqual(PairSetList<Integer, Integer> expectedPairs) {
		// NB: even if Integer can't be inherited, PairIterator is a Iterator<? extends X, ? extends X>
		List<Duo<Integer, Integer>> obtainedPairs = PairSetList.toPairs(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		List<Set<Duo<Integer, Integer>>> obtained = new ArrayList<>();
		int startIndex = 0;
		for (Set<Duo<Integer, Integer>> expectedPair : expectedPairs.asList()) {
			obtained.add(new HashSet<>(obtainedPairs.subList(startIndex, startIndex += expectedPair.size())));
		}
		assertThat(obtained).isEqualTo(expectedPairs.asList());
	}
	
	@Nested
	class CRUD {
		
		@BeforeEach
		void setUp() throws SQLException {
			initMapping();
			initTest();
		}
		
		@Test
		void insert() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1, 1, 1 }, new long[] { 1 }, new long[] { 1, 1, 1 }, new long[] { 1 }));
			testInstance.insert(Arrays.asList(
					new Toto(17, 23, 117, 123, -117),
					new Toto(29, 31, 129, 131, -129),
					new Toto(37, 41, 137, 141, -137),
					new Toto(43, 53, 143, 153, -143)
			));
			
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("insert into Toto1(id, a, b) values (?, ?, ?)"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 17).add(3, 23)
					.newRow(1, 2).add(2, 29).add(3, 31)
					.newRow(1, 3).add(2, 37).add(3, 41)
					.newRow(1, 4).add(2, 43).add(3, 53);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void update() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto1_id";
			String totoAAlias = "Toto1_a";
			String totoBAlias = "Toto1_b";
			ResultSet resultSetMock = new InMemoryResultSet(Arrays.asList(
					Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 13).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 17).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 23).add(totoAAlias, 1).add(totoBAlias, 2)
			));
			when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
			
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 1 }));
			testInstance.update(Arrays.asList(
					new Toto(7, 17, 23),
					new Toto(13, 29, 31),
					new Toto(17, 37, 41),
					new Toto(23, 43, 53)
			));
			
			// 2 queries because we select 4 entities, to be spread over in(..) operator that has a maximum size of 3
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(16)).setInt(indexCaptor.capture(), valueCaptor.capture());
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ " from Toto1 where Toto1.id in (?, ?, ?)",
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ " from Toto1 where Toto1.id in (?)",
					"update Toto1 set a = ?, b = ? where id = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 7).add(2, 13).add(3, 17).add(1, 23);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void updateById() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 1 }, new long[] { 3 }, new long[] { 1 }));
			testInstance.updateById(Arrays.asList(
					new Toto(1, 17, 23, 117, 123, -117),
					new Toto(2, 29, 31, 129, 131, -129),
					new Toto(3, 37, 41, 137, 141, -137),
					new Toto(4, 43, 53, 143, 153, -143)
			));
			
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("update Toto1 set a = ?, b = ? where id = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 17).add(2, 23).add(3, 1)
					.newRow(1, 29).add(2, 31).add(3, 2)
					.newRow(1, 37).add(2, 41).add(3, 3)
					.newRow(1, 43).add(2, 53).add(3, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void delete() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1 }, new long[] { 1 }));
			testInstance.delete(Arrays.asList(new Toto(7, 17, 23, 117, 123, -117)));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto1 where id = ?"));
			verify(preparedStatement, times(1)).addBatch();
			verify(preparedStatement, times(1)).executeLargeBatch();
			verify(preparedStatement, times(0)).executeUpdate();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 7);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void delete_multiple() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 1 }, new long[] { 3 }, new long[] { 1 }));
			testInstance.delete(Arrays.asList(
					new Toto(1, 17, 23, 117, 123, -117),
					new Toto(2, 29, 31, 129, 131, -129),
					new Toto(3, 37, 41, 137, 141, -137),
					new Toto(4, 43, 53, 143, 153, -143)
			));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto1 where id = ?"));
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			verify(preparedStatement, times(0)).executeUpdate();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(1, 2).add(1, 3)
					.newRow(1, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void deleteById() throws SQLException {
			expectedRowCountForUpdate.set(1L);
			testInstance.deleteById(Arrays.asList(
					new Toto(7, 17, 23, 117, 123, -117)
			));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto1 where id in (?)"));
			verify(preparedStatement, times(1)).executeLargeUpdate();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 7);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void deleteById_multiple() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 3 }));
			expectedRowCountForUpdate.set(1L);
			testInstance.deleteById(Arrays.asList(
					new Toto(1, 17, 23, 117, 123, -117),
					new Toto(2, 29, 31, 129, 131, -129),
					new Toto(3, 37, 41, 137, 141, -137),
					new Toto(4, 43, 53, 143, 153, -143)
			));
			// 4 statements because in operator is bounded to 3 values (see testInstance creation)
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto1 where id in (?, ?, ?)",
																				  "delete from Toto1 where id in (?)"));
			verify(preparedStatement, times(1)).addBatch();
			verify(preparedStatement, times(1)).executeLargeBatch();
			verify(preparedStatement, times(1)).executeLargeUpdate();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2).add(3, 3)
					.newRow(1, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void select() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto1_id";
			String totoAAlias = "Toto1_a";
			String totoBAlias = "Toto1_b";
			ResultSet resultSetMock = new InMemoryResultSet(Arrays.asList(
					Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 13).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 17).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 23).add(totoAAlias, 1).add(totoBAlias, 2)
			));
			when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
			
			List<Toto> select = testInstance.select(Arrays.asList(
					new PersistableIdentifier<>(7),
					new PersistableIdentifier<>(13),
					new PersistableIdentifier<>(17),
					new PersistableIdentifier<>(23)
			));
			
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ " from Toto1 where Toto1.id in (?, ?, ?)",
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ " from Toto1 where Toto1.id in (?)"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 7).add(2, 13).add(3, 17).add(1, 23);
			assertCapturedPairsEqual(expectedPairs);
			
			Comparator<Toto> totoComparator = Comparator.<Toto, Comparable>comparing(toto -> toto.getId().getSurrogate());
			assertThat(Arrays.asTreeSet(totoComparator, select).toString()).isEqualTo(Arrays.asTreeSet(totoComparator,
																									   new Toto(7, 1, 2),
																									   new Toto(13, 1, 2),
																									   new Toto(17, 1, 2),
																									   new Toto(23, 1, 2)
			).toString());
		}
		
		@Test
		void selectWhere() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto1_id";
			String totoAAlias = "Toto1_a";
			String totoBAlias = "Toto1_b";
			ResultSet resultSetForCriteria = new InMemoryResultSet(Arrays.asList(
					Maps.asMap("rootId", (Object) 7)
			));
			ResultSet resultSetForFinalResult = new InMemoryResultSet(Arrays.asList(
					Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2)
			));
			ModifiableInt queryCounter = new ModifiableInt();
			when(preparedStatement.executeQuery()).thenAnswer((Answer<ResultSet>) invocation -> {
				queryCounter.increment();
				switch (queryCounter.getValue()) {
					case 1:
						return resultSetForCriteria;
					case 2:
						return resultSetForFinalResult;
				}
				return null;
			});
			
			RelationalExecutableEntityQuery<Toto> totoRelationalExecutableEntityQuery = testInstance.selectWhere(Toto::getA, Operators.eq(42));
			List<Toto> select = totoRelationalExecutableEntityQuery.execute();
			
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto1.id as rootId from Toto1 where (Toto1.a = ?)",
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ " from Toto1 where Toto1.id in (?)"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 42);
			assertCapturedPairsEqual(expectedPairs);
			
			Comparator<Toto> totoComparator = Comparator.<Toto, Comparable>comparing(toto -> toto.getId().getSurrogate());
			assertThat(Arrays.asTreeSet(totoComparator, select).toString()).isEqualTo(Arrays.asTreeSet(totoComparator,
																									   new Toto(7, 1, 2)
			).toString());
		}
	}
	
	@Nested
	class CRUD_WithListener {
		
		private ClassMappingStrategy<Toto, Identifier<Integer>, Table> totoClassMappingStrategy2_ontoTable2;
		private Persister<Toto, Identifier<Integer>, ?> persister2;
		
		@BeforeEach
		void setUp() throws SQLException {
			initMapping();
			initTest();
		}
		
		void initMapping() {
			SimpleRelationalEntityPersisterTest.this.initMapping();
			
			Field fieldId = Reflections.getField(Toto.class, "id");
			Field fieldX = Reflections.getField(Toto.class, "x");
			Field fieldY = Reflections.getField(Toto.class, "y");
			Field fieldZ = Reflections.getField(Toto.class, "z");
			
			Table totoClassTable2 = new Table("Toto2");
			rightJoinColumn = totoClassTable2.addColumn("id", fieldId.getType());
			totoClassTable2.addColumn("x", fieldX.getType());
			totoClassTable2.addColumn("y", fieldY.getType());
			totoClassTable2.addColumn("z", fieldZ.getType());
			Map<String, Column> columnMap2 = totoClassTable2.mapColumnsOnName();
			columnMap2.get("id").setPrimaryKey(true);
			
			PropertyAccessor<Toto, Identifier<Integer>> identifierAccessor = Accessors.propertyAccessor(fieldId);
			Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> totoClassMapping2 = Maps.asMap(
							(PropertyAccessor) identifierAccessor, (Column<Table, Object>) columnMap2.get("id"))
					.add(Accessors.propertyAccessor(fieldX), columnMap2.get("x"))
					.add(Accessors.propertyAccessor(fieldY), columnMap2.get("y"))
					.add(Accessors.propertyAccessor(fieldZ), columnMap2.get("z"));
			
			AlreadyAssignedIdentifierManager<Toto, Identifier<Integer>> identifierManager =
					new AlreadyAssignedIdentifierManager<>((Class<Identifier<Integer>>) (Class) Identifier.class,
														   c -> c.getId().setPersisted(), c -> c.getId().isPersisted());
			totoClassMappingStrategy2_ontoTable2 = new ClassMappingStrategy<>(Toto.class, totoClassTable2,
																			  totoClassMapping2, identifierAccessor,
																			  identifierManager);
		}
		
		protected void initTest() throws SQLException {
			SimpleRelationalEntityPersisterTest.this.initTest();
			
			// we add a copier onto a another table
			persister2 = new Persister<>(totoClassMappingStrategy2_ontoTable2, dialect, new ConnectionConfigurationSupport(() -> connection, 3));
			testInstance.getEntityJoinTree().addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
															 new EntityMappingStrategyAdapter<>(persister2.getMappingStrategy()),
															 leftJoinColumn, rightJoinColumn, null, JoinType.INNER, Toto::merge, Collections.emptySet());
			testInstance.getPersisterListener().addInsertListener(new InsertListener<Toto>() {
				@Override
				public void afterInsert(Iterable<? extends Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister2.insert(entities);
				}
			});
			testInstance.getPersisterListener().addUpdateListener(new UpdateListener<Toto>() {
				@Override
				public void afterUpdate(Iterable<? extends Duo<? extends Toto, ? extends Toto>> entities, boolean allColumnsStatement) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister2.update((Iterable<? extends Duo<Toto, Toto>>) entities, allColumnsStatement);
				}
			});
			testInstance.getPersisterListener().addUpdateByIdListener(new UpdateByIdListener<Toto>() {
				@Override
				public void afterUpdateById(Iterable<Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister2.updateById(entities);
				}
			});
			testInstance.getPersisterListener().addDeleteListener(new DeleteListener<Toto>() {
				@Override
				public void beforeDelete(Iterable<Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister2.delete(entities);
				}
			});
			testInstance.getPersisterListener().addDeleteByIdListener(new DeleteByIdListener<Toto>() {
				@Override
				public void beforeDeleteById(Iterable<Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister2.deleteById(entities);
				}
			});
		}
		
		@Test
		void insert() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1, 1, 1 }, new long[] { 1 }, new long[] { 1, 1, 1 }, new long[] { 1 }));
			testInstance.insert(Arrays.asList(
					new Toto(17, 23, 117, 123, -117),
					new Toto(29, 31, 129, 131, -129),
					new Toto(37, 41, 137, 141, -137),
					new Toto(43, 53, 143, 153, -143)
			));
			
			verify(preparedStatement, times(8)).addBatch();
			verify(preparedStatement, times(4)).executeLargeBatch();
			verify(preparedStatement, times(28)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("insert into Toto1(id, a, b) values (?, ?, ?)", "insert into Toto2(id,"
					+ " x, y, z) values (?, ?, ?, ?)"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 17).add(3, 23)
					.newRow(1, 2).add(2, 29).add(3, 31)
					.newRow(1, 3).add(2, 37).add(3, 41)
					.newRow(1, 4).add(2, 43).add(3, 53)
					
					.newRow(1, 1).add(2, 117).add(3, 123).add(4, -117)
					.newRow(1, 2).add(2, 129).add(3, 131).add(4, -129)
					.newRow(1, 3).add(2, 137).add(3, 141).add(4, -137)
					.newRow(1, 4).add(2, 143).add(3, 153).add(4, -143);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void update() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto1_id";
			String totoAAlias = "Toto1_a";
			String totoBAlias = "Toto1_b";
			String toto2IdAlias = "Toto2_id";
			String toto2XAlias = "Toto2_x";
			String toto2YAlias = "Toto2_y";
			String toto2ZAlias = "Toto2_z";
			ResultSet resultSetMock = new InMemoryResultSet(Arrays.asList(
					Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6),
					Maps.asMap(totoIdAlias, (Object) 13).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6),
					Maps.asMap(totoIdAlias, (Object) 17).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6),
					Maps.asMap(totoIdAlias, (Object) 23).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6)
			));
			when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
			
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 1 }, new long[] { 3 }, new long[] { 1 }));
			testInstance.update(Arrays.asList(
					new Toto(7, 17, 23, 117, 123, -117),
					new Toto(13, 29, 31, 129, 131, -129),
					new Toto(17, 37, 41, 137, 141, -137),
					new Toto(23, 43, 53, 143, 153, -143)
			));
			
			// 2 queries because we select 4 entities, to be spread over in(..) operator that has a maximum size of 3
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(32)).setInt(indexCaptor.capture(), valueCaptor.capture());
			verify(preparedStatement, times(8)).addBatch();
			verify(preparedStatement, times(4)).executeLargeBatch();
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ ", Toto2.z as " + toto2ZAlias
							+ ", Toto2.x as " + toto2XAlias
							+ ", Toto2.y as " + toto2YAlias
							+ ", Toto2.id as " + toto2IdAlias
							+ " from Toto1 inner join Toto2 as Toto2 on Toto1.id = Toto2.id where Toto1.id in (?, ?, ?)",
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ ", Toto2.z as " + toto2ZAlias
							+ ", Toto2.x as " + toto2XAlias
							+ ", Toto2.y as " + toto2YAlias
							+ ", Toto2.id as " + toto2IdAlias
							+ " from Toto1 inner join Toto2 as Toto2 on Toto1.id = Toto2.id where Toto1.id in (?)",
					"update Toto1 set a = ?, b = ? where id = ?",
					"update Toto2 set x = ?, y = ?, z = ? where id = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 7).add(2, 13).add(3, 17).add(1, 23);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void updateById() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 1 }, new long[] { 3 }, new long[] { 1 }));
			testInstance.updateById(Arrays.asList(
					new Toto(1, 17, 23, 117, 123, -117),
					new Toto(2, 29, 31, 129, 131, -129),
					new Toto(3, 37, 41, 137, 141, -137),
					new Toto(4, 43, 53, 143, 153, -143)
			));
			
			verify(preparedStatement, times(8)).addBatch();
			verify(preparedStatement, times(4)).executeLargeBatch();
			verify(preparedStatement, times(28)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("update Toto1 set a = ?, b = ? where id = ?", "update Toto2 set x = ?,"
					+ " y = ?, z = ? where id = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 17).add(2, 23).add(3, 1)
					.newRow(1, 29).add(2, 31).add(3, 2)
					.newRow(1, 37).add(2, 41).add(3, 3)
					.newRow(1, 43).add(2, 53).add(3, 4)
					
					.newRow(1, 117).add(2, 123).add(3, -117).add(4, 1)
					.newRow(1, 129).add(2, 131).add(3, -129).add(4, 2)
					.newRow(1, 137).add(2, 141).add(3, -137).add(4, 3)
					.newRow(1, 143).add(2, 153).add(3, -143).add(4, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void delete() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1 }, new long[] { 1 }));
			testInstance.delete(Arrays.asList(new Toto(7, 17, 23, 117, 123, -117)));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto2 where id = ?", "delete from Toto1 where id = ?"));
			verify(preparedStatement, times(2)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			verify(preparedStatement, times(0)).executeUpdate();
			verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 7)
					.newRow(1, 7);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void delete_multiple() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 1 }, new long[] { 3 }, new long[] { 1 }));
			testInstance.delete(Arrays.asList(
					new Toto(1, 17, 23, 117, 123, -117),
					new Toto(2, 29, 31, 129, 131, -129),
					new Toto(3, 37, 41, 137, 141, -137),
					new Toto(4, 43, 53, 143, 153, -143)
			));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto2 where id = ?", "delete from Toto1 where id = ?"));
			verify(preparedStatement, times(8)).addBatch();
			verify(preparedStatement, times(4)).executeLargeBatch();
			verify(preparedStatement, times(0)).executeUpdate();
			verify(preparedStatement, times(8)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(1, 2).add(1, 3)
					.newRow(1, 4)
					.newRow(1, 1).add(1, 2).add(1, 3)
					.newRow(1, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void deleteById() throws SQLException {
			expectedRowCountForUpdate.set(1L);
			testInstance.deleteById(Arrays.asList(
					new Toto(7, 17, 23, 117, 123, -117)
			));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto2 where id in (?)", "delete from Toto1 where id in (?)"));
			verify(preparedStatement, times(2)).executeLargeUpdate();
			verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 7);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void deleteById_multiple() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 3 }, new long[] { 3 }, new long[] { 1 }, new long[] { 1 }));
			expectedRowCountForUpdate.set(1L);
			testInstance.deleteById(Arrays.asList(
					new Toto(1, 17, 23, 117, 123, -117),
					new Toto(2, 29, 31, 129, 131, -129),
					new Toto(3, 37, 41, 137, 141, -137),
					new Toto(4, 43, 53, 143, 153, -143)
			));
			// 4 statements because in operator is bounded to 3 values (see testInstance creation)
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto2 where id in (?, ?, ?)",
																				  "delete from Toto2 where id in (?)",
																				  "delete from Toto1 where id in (?, ?, ?)",
																				  "delete from Toto1 where id in (?)"));
			verify(preparedStatement, times(2)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			verify(preparedStatement, times(2)).executeLargeUpdate();
			verify(preparedStatement, times(8)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2).add(3, 3)
					.newRow(1, 4)
					.newRow(1, 1).add(2, 2).add(3, 3)
					.newRow(1, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void select() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto1_id";
			String totoAAlias = "Toto1_a";
			String totoBAlias = "Toto1_b";
			String toto2IdAlias = "Toto2_id";
			String toto2XAlias = "Toto2_x";
			String toto2YAlias = "Toto2_y";
			String toto2ZAlias = "Toto2_z";
			ResultSet resultSetMock = new InMemoryResultSet(Arrays.asList(
					Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6),
					Maps.asMap(totoIdAlias, (Object) 13).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6),
					Maps.asMap(totoIdAlias, (Object) 17).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6),
					Maps.asMap(totoIdAlias, (Object) 23).add(totoAAlias, 1).add(totoBAlias, 2).add(toto2IdAlias, 3).add(toto2XAlias, 4).add(toto2YAlias, 5).add(toto2ZAlias, 6)
			));
			when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
			
			List<Toto> select = testInstance.select(Arrays.asList(
					new PersistableIdentifier<>(7),
					new PersistableIdentifier<>(13),
					new PersistableIdentifier<>(17),
					new PersistableIdentifier<>(23)
			));
			
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ ", Toto2.z as " + toto2ZAlias
							+ ", Toto2.x as " + toto2XAlias
							+ ", Toto2.y as " + toto2YAlias
							+ ", Toto2.id as " + toto2IdAlias
							+ " from Toto1 inner join Toto2 as Toto2 on Toto1.id = Toto2.id where Toto1.id in (?, ?, ?)",
					"select Toto1.id as " + totoIdAlias
							+ ", Toto1.a as " + totoAAlias
							+ ", Toto1.b as " + totoBAlias
							+ ", Toto2.z as " + toto2ZAlias
							+ ", Toto2.x as " + toto2XAlias
							+ ", Toto2.y as " + toto2YAlias
							+ ", Toto2.id as " + toto2IdAlias
							+ " from Toto1 inner join Toto2 as Toto2 on Toto1.id = Toto2.id where Toto1.id in (?)"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 7).add(2, 13).add(3, 17).add(1, 23);
			assertCapturedPairsEqual(expectedPairs);
			
			Comparator<Toto> totoComparator = Comparator.<Toto, Comparable>comparing(toto -> toto.getId().getSurrogate());
			assertThat(Arrays.asTreeSet(totoComparator, select).toString()).isEqualTo(Arrays.asTreeSet(totoComparator,
																									   new Toto(7, 1, 2, 4, 5, 6),
																									   new Toto(13, 1, 2, 4, 5, 6),
																									   new Toto(17, 1, 2, 4, 5, 6),
																									   new Toto(23, 1, 2, 4, 5, 6)
			).toString());
		}
	}
	
	private static class Toto implements Identified<Integer> {
		private Identifier<Integer> id;
		private Integer a, b, x, y, z;
		
		public Toto() {
		}
		
		public Toto(int id, Integer a, Integer b) {
			this(id, a, b, null, null, null);
		}
		
		public Toto(int id, Integer a, Integer b, Integer x, Integer y, Integer z) {
			this.id = new PersistableIdentifier<>(id);
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
		public Identifier<Integer> getId() {
			return id;
		}
		
		public Integer getA() {
			return a;
		}
		
		/**
		 * Method to merge another bean with this one on a part of their attributes.
		 * It has no real purpose, it only exists to fullfill the relational mapping between tables Toto and Toto2 and avoid a NullPointerException
		 * when associating 2 results of RowTransformer
		 * 
		 * @param another a bean coming from the persister2
		 */
		void merge(Toto another) {
			this.x = another.x;
			this.y = another.y;
			this.z = another.z;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", (Object) id.getSurrogate()).add("a", a).add("b", b).add("x", x).add("y", y).add("z", z)
					+ "]";
		}
	}
}