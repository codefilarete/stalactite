package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Array;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.InMemoryCounterIdentifierGenerator;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.PairIterator.EmptyIterator;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.tool.trace.ModifiableInt;
import org.codefilarete.tool.trace.ModifiableLong;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy.alreadyAssigned;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doNothing;
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
	private ClassMapping<Toto, Identifier<Integer>, ?> totoClassMappingStrategy_ontoTable1;
	private Dialect dialect;
	private Key<Table, Identifier<Integer>> leftJoinColumn;
	private Key<Table, Identifier<Integer>> rightJoinColumn;
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
		Field fieldQ = Reflections.getField(Toto.class, "q");
		
		Table totoClassTable = new Table("Toto");
		leftJoinColumn = Key.ofSingleColumn(totoClassTable.addColumn("id", fieldId.getType()));
		totoClassTable.addColumn("a", fieldA.getType());
		totoClassTable.addColumn("b", fieldB.getType());
		totoClassTable.addColumn("q", fieldQ.getType());
		Map<String, Column> columnMap = totoClassTable.mapColumnsOnName();
		columnMap.get("id").setPrimaryKey(true);
		
		PropertyAccessor<Toto, Identifier<Integer>> identifierAccessor = Accessors.propertyAccessor(fieldId);
		Map<PropertyAccessor<Toto, Object>, Column<Table, Object>> totoClassMapping = Maps.asMap(
						(PropertyAccessor) identifierAccessor, (Column<Table, Object>) columnMap.get("id"))
				.add(Accessors.propertyAccessor(fieldA), columnMap.get("a"))
				.add(Accessors.propertyAccessor(fieldB), columnMap.get("b"))
				.add(Accessors.propertyAccessor(fieldQ), columnMap.get("q"));
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		
		BeforeInsertIdentifierManager<Toto, Identifier<Integer>> beforeInsertIdentifierManager = new BeforeInsertIdentifierManager<>(
				new AccessorWrapperIdAccessor<>(identifierAccessor),
				() -> new PersistableIdentifier<>(identifierGenerator.next()),
				(Class<Identifier<Integer>>) (Class) Identifier.class);
		totoClassMappingStrategy_ontoTable1 = new ClassMapping<>(Toto.class,
				totoClassTable,
				totoClassMapping,
				identifierAccessor,
				beforeInsertIdentifierManager);
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Identifier.class, "int");
		
		dialect = new Dialect(simpleTypeMapping);
		dialect.setInOperatorMaxSize(3);
		dialect.getColumnBinderRegistry().register(Identifier.class, new ParameterBinder<Identifier>() {
			@Override
			public Class<Identifier> getType() {
				return Identifier.class;
			}
			
			@Override
			public Identifier doGet(ResultSet resultSet, String columnName) throws SQLException {
				return new PersistedIdentifier<>(resultSet.getObject(columnName));
			}
			
			@Override
			public void set(PreparedStatement statement, int valueIndex, Identifier value) throws SQLException {
				statement.setInt(valueIndex, (Integer) value.getSurrogate());
			}
		});
		// Registering a binder of Set for the Toto.q property
		dialect.getColumnBinderRegistry().register((Class<Set<Integer>>) (Class) Set.class, new ParameterBinder<Set<Integer>>() {
			@Override
			public void set(PreparedStatement preparedStatement, int valueIndex, Set<Integer> value) throws SQLException {
				if (value != null) {
					preparedStatement.setArray(valueIndex, new JDBCArrayBasic(value.toArray(new Integer[0]), Type.SQL_INTEGER));
				} else {
					preparedStatement.setArray(valueIndex, null);
				}
			}
			
			@Override
			public Set<Integer> doGet(ResultSet resultSet, String columnName) throws SQLException {
				Array array = resultSet.getArray(columnName);
				return array == null ? null : new KeepOrderSet<>((Integer[]) array.getArray());
			}
			
			@Override
			public Class<Set<Integer>> getType() {
				return (Class<Set<Integer>>) (Class) Set.class;
			}
		});
	}
	
	protected void initTest() throws SQLException {
		// reset id counter between 2 tests to keep independence between them
		identifierGenerator.reset();
		
		preparedStatement = mock(PreparedStatement.class);
		// we set row count else it will throw exception
		when(preparedStatement.executeLargeBatch()).thenAnswer((Answer<long[]>) invocation -> effectiveBatchedRowCount.next());
		when(preparedStatement.executeLargeUpdate()).thenAnswer((Answer<Long>) invocation -> expectedRowCountForUpdate.get());
		
		connection = mock(Connection.class);
		// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
		// either or not it should prepare statement
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
			PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext(mock(PersisterRegistry.class)));
			initMapping();
			initTest();
			PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
			PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
		}
		
		@AfterEach
		void removeEntityCandidates() {
			PersisterBuilderContext.CURRENT.remove();
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
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("insert into Toto(id, a, b, q) values (?, ?, ?, ?)"));
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
			String totoIdAlias = "Toto_id";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoQAlias = "Toto_q";
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
					"select Toto.id as " + totoIdAlias
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.q as " + totoQAlias
							+ " from Toto where Toto.id in (?, ?, ?)",
					"select Toto.id as " + totoIdAlias
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.q as " + totoQAlias
							+ " from Toto where Toto.id in (?)",
					"update Toto set a = ?, b = ?, q = ? where id = ?"));
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
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("update Toto set a = ?, b = ?, q = ? where id = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					// since PreparedStatement.setArray is not watched we don't have q property index (3)
					.newRow(1, 17).add(2, 23).add(4, 1)
					.newRow(1, 29).add(2, 31).add(4, 2)
					.newRow(1, 37).add(2, 41).add(4, 3)
					.newRow(1, 43).add(2, 53).add(4, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void delete() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1 }, new long[] { 1 }));
			testInstance.delete(Arrays.asList(new Toto(7, 17, 23, 117, 123, -117)));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where id = ?"));
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
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where id = ?"));
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
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where id in (?)"));
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
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where id in (?, ?, ?)",
					"delete from Toto where id in (?)"));
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
			String totoIdAlias = "Toto_id";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoQAlias = "Toto_q";
			ResultSet resultSetMock = new InMemoryResultSet(Arrays.asList(
					Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 13).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 17).add(totoAAlias, 1).add(totoBAlias, 2),
					Maps.asMap(totoIdAlias, (Object) 23).add(totoAAlias, 1).add(totoBAlias, 2)
			));
			when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
			
			Set<Toto> select = testInstance.select(Arrays.asList(
					new PersistableIdentifier<>(7),
					new PersistableIdentifier<>(13),
					new PersistableIdentifier<>(17),
					new PersistableIdentifier<>(23)
			));
			
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + totoIdAlias
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.q as " + totoQAlias
							+ " from Toto where Toto.id in (?, ?, ?)",
					"select Toto.id as " + totoIdAlias
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.q as " + totoQAlias
							+ " from Toto where Toto.id in (?)"));
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
			String totoIdAlias = "Toto_id";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoQAlias = "Toto_q";
			ResultSet resultSetForCriteria = new InMemoryResultSet(Arrays.asList(
					Maps.asMap(totoIdAlias, (Object) 7).add(totoAAlias, 1).add(totoBAlias, 2)
			));
			when(preparedStatement.executeQuery()).thenReturn(resultSetForCriteria);
			
			ExecutableEntityQueryCriteria<Toto> totoExecutableEntityQueryCriteria = testInstance.selectWhere(Toto::getA, Operators.eq(42));
			Set<Toto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + totoIdAlias
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.q as " + totoQAlias
							+ " from Toto where Toto.a = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 42);
			assertCapturedPairsEqual(expectedPairs);
			
			Comparator<Toto> totoComparator = Comparator.<Toto, Comparable>comparing(toto -> toto.getId().getSurrogate());
			assertThat(Arrays.asTreeSet(totoComparator, select).toString()).isEqualTo(Arrays.asTreeSet(totoComparator,
					new Toto(7, 1, 2)
			).toString());
		}
		
		@Test
		void selectWhere_collectionCriteria() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto_id";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoQAlias = "Toto_q";
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
			
			// We test the collection criteria through the "q" property which is not really a production use case since it's an "embedded" one
			// made of the storage of a set in an SQL Array : production use case we'll be more a *-to-many case. Meanwhile, this test should pass
			// because even in that case it's hard to retrieve the entities with one select
			ExecutableEntityQueryCriteria<Toto> totoExecutableEntityQueryCriteria = testInstance.selectWhere(Toto::getQ, Operators.eq(Arrays.asHashSet(42)));
			Set<Toto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as rootId from Toto where Toto.q = ?",
					"select Toto.id as " + totoIdAlias
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.q as " + totoQAlias
							+ " from Toto where Toto.id in (?)"));
			// Here we don't test the "42" row of the first query because it requires to listen to PreparedStatement.setArray(..) whereas all this
			// test class is bound to PreparedStatement.setInt(..) and Integer type.
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 7);
			assertCapturedPairsEqual(expectedPairs);
			
			Comparator<Toto> totoComparator = Comparator.<Toto, Comparable>comparing(toto -> toto.getId().getSurrogate());
			assertThat(Arrays.asTreeSet(totoComparator, select).toString()).isEqualTo(Arrays.asTreeSet(totoComparator,
					new Toto(7, 1, 2)
			).toString());
		}
	}
		
	@Nested
	class LoadByEntityCriteria {
		
		@Test
		void checkSQLGeneration() throws SQLException {
			
			PreparedStatement preparedStatement = mock(PreparedStatement.class);
			Connection connection = mock(Connection.class);
			ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
			ArgumentCaptor<Integer> argCaptor = ArgumentCaptor.forClass(Integer.class);
			when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatement);
			doNothing().when(preparedStatement).setInt(anyInt(), argCaptor.capture());
			DataSource dataSource = mock(DataSource.class);
			when(dataSource.getConnection()).thenReturn(connection);
			when(preparedStatement.executeQuery()).thenReturn(new InMemoryResultSet(new EmptyIterator<>()));
			
			
			PersistenceContext persistenceContext = new PersistenceContext(dataSource, new HSQLDBDialect());
			FluentEntityMappingBuilder<Tata, Integer> mappingConfiguration = entityBuilder(Tata.class, Integer.class)
					.mapKey(Tata::getId, alreadyAssigned(tata -> {}, tata -> false))	// identifier policy doesn't matter
					.map(Tata::getProp1);
			
			EntityPersister<Toto, Integer> totoPersister = entityBuilder(Toto.class, Integer.class)
					.mapKey(Toto::getA, alreadyAssigned(toto -> {}, toto -> false))
					.mapOneToOne(Toto::getTata, mappingConfiguration)
					.build(persistenceContext);
			
			SerializableFunction<Toto, Tata> getTata = Toto::getTata;
			SerializableFunction<Tata, String> getProp1 = Tata::getProp1;
			Equals<String> dummy = Operators.eq("dummy");
			EntityPersister.ExecutableEntityQuery<Toto> totoRelationalExecutableEntityQuery = totoPersister
					.selectWhere(Toto::getA, Operators.eq(42))
					.and(getTata, getProp1, dummy);
			
			totoRelationalExecutableEntityQuery.execute(Accumulators.toSet());
			
			assertThat(sqlCaptor.getValue()).isEqualTo("select Toto.a as Toto_a, tata.prop1 as tata_prop1, tata.id as tata_id from Toto left outer join Tata as tata on Toto.tataId = tata.id where Toto.a = ? and tata.prop1 = ?");
			assertThat(argCaptor.getValue()).isEqualTo(42);
		}
	}
	
	@Nested
	class LoadProjectionByEntityCriteria {
		
		@BeforeEach
		void setUp() throws SQLException {
			PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext(mock(PersisterRegistry.class)));
			initMapping();
			initTest();
			PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
			PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
		}
		
		@AfterEach
		void removeEntityCandidates() {
			PersisterBuilderContext.CURRENT.remove();
		}
		
		@Test
		void selectProjectionWhere() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			ResultSet resultSet = new InMemoryResultSet(Arrays.asList(
					Maps.asMap("count", 42L)
			));
			when(preparedStatement.executeQuery()).thenAnswer((Answer<ResultSet>) invocation -> resultSet);
			
			Count count = Operators.count(new SelectableString<>("id", Integer.class));
			ExecutableProjectionQuery<Toto> totoRelationalExecutableEntityQuery = testInstance.selectProjectionWhere(select ->  {
				select.clear();
				select.add(count, "count");
			}, Toto::getA, Operators.eq(77));
			long countValue = totoRelationalExecutableEntityQuery.execute(new Accumulator<Function<Selectable<Long>, Long>, ModifiableLong, Long>() {
				@Override
				public Supplier<ModifiableLong> supplier() {
					return ModifiableLong::new;
				}
				
				@Override
				public BiConsumer<ModifiableLong, Function<Selectable<Long>, Long>> aggregator() {
					return (modifiableInt, selectableObjectFunction) -> {
						Long apply = selectableObjectFunction.apply(count);
						modifiableInt.reset(apply);
					};
				}
				
				@Override
				public Function<ModifiableLong, Long> finisher() {
					return ModifiableLong::getValue;
				}
			});
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select count(id) as count from Toto where Toto.a = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 77);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(countValue).isEqualTo(42);
		}
	}
		
	@Nested
	class CRUD_WithListener {
		
		private ClassMapping<Toto, Identifier<Integer>, ?> totoClassMappingStrategy;
		private BeanPersister<Toto, Identifier<Integer>, ?> persister;
		
		@BeforeEach
		void setUp() throws SQLException {
			PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext(mock(PersisterRegistry.class)));
			initMapping();
			initTest();
			PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
			PersisterBuilderContext.CURRENT.get().getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
		}
		
		@AfterEach
		void removeEntityCandidates() {
			PersisterBuilderContext.CURRENT.remove();
		}
		
		void initMapping() {
			SimpleRelationalEntityPersisterTest.this.initMapping();
			
			Field fieldId = Reflections.getField(Toto.class, "id");
			Field fieldX = Reflections.getField(Toto.class, "x");
			Field fieldY = Reflections.getField(Toto.class, "y");
			Field fieldZ = Reflections.getField(Toto.class, "z");
			
			Table totoClassTable = new Table("Toto2");
			rightJoinColumn = Key.ofSingleColumn(totoClassTable.addColumn("id", fieldId.getType()));
			totoClassTable.addColumn("x", fieldX.getType());
			totoClassTable.addColumn("y", fieldY.getType());
			totoClassTable.addColumn("z", fieldZ.getType());
			Map<String, Column> columnMap2 = totoClassTable.mapColumnsOnName();
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
			totoClassMappingStrategy = new ClassMapping<>(Toto.class, totoClassTable, totoClassMapping2, identifierAccessor, identifierManager);
		}
		
		protected void initTest() throws SQLException {
			SimpleRelationalEntityPersisterTest.this.initTest();
			
			// we add a copier onto a another table
			persister = new BeanPersister<>(totoClassMappingStrategy, dialect, new ConnectionConfigurationSupport(() -> connection, 3));
			testInstance.getEntityJoinTree().addRelationJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
															 new EntityMappingAdapter<>(persister.getMapping()),
															 leftJoinColumn, rightJoinColumn, null, JoinType.INNER, Toto::merge, Collections.emptySet());
			testInstance.getPersisterListener().addInsertListener(new InsertListener<Toto>() {
				@Override
				public void afterInsert(Iterable<? extends Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister.insert(entities);
				}
			});
			testInstance.getPersisterListener().addUpdateListener(new UpdateListener<Toto>() {
				@Override
				public void afterUpdate(Iterable<? extends Duo<Toto, Toto>> entities, boolean allColumnsStatement) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister.update(entities, allColumnsStatement);
				}
			});
			testInstance.getPersisterListener().addUpdateByIdListener(new UpdateByIdListener<Toto>() {
				@Override
				public void afterUpdateById(Iterable<? extends Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister.updateById(entities);
				}
			});
			testInstance.getPersisterListener().addDeleteListener(new DeleteListener<Toto>() {
				@Override
				public void beforeDelete(Iterable<? extends Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister.delete(entities);
				}
			});
			testInstance.getPersisterListener().addDeleteByIdListener(new DeleteByIdListener<Toto>() {
				@Override
				public void beforeDeleteById(Iterable<? extends Toto> entities) {
					// since we only want a replicate of totos in table2, we only need to return them
					persister.deleteById(entities);
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
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"insert into Toto(id, a, b, q) values (?, ?, ?, ?)",
					"insert into Toto2(id, x, y, z) values (?, ?, ?, ?)"));
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
			String totoIdAlias = "Toto_id";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoQAlias = "Toto_q";
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
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(0)).getColumns()).containsExactlyInAnyOrder(
					"Toto.id as " + totoIdAlias,
							"Toto.a as " + totoAAlias,
							"Toto.b as " + totoBAlias,
							"Toto.q as " + totoQAlias,
							"Toto2.z as " + toto2ZAlias,
							"Toto2.x as " + toto2XAlias,
							"Toto2.y as " + toto2YAlias,
							"Toto2.id as " + toto2IdAlias
			);
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(0)).getFrom()).isEqualTo(
					"Toto inner join Toto2 as Toto2 on Toto.id = Toto2.id where Toto.id in (?, ?, ?)"
			);
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(1)).getColumns()).containsExactlyInAnyOrder(
					"Toto.id as " + totoIdAlias,
							"Toto.a as " + totoAAlias,
							"Toto.b as " + totoBAlias,
							"Toto.q as " + totoQAlias,
							"Toto2.z as " + toto2ZAlias,
							"Toto2.x as " + toto2XAlias,
							"Toto2.y as " + toto2YAlias,
							"Toto2.id as " + toto2IdAlias
			);
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(1)).getFrom()).isEqualTo(
					"Toto inner join Toto2 as Toto2 on Toto.id = Toto2.id where Toto.id in (?)"
			);
			assertThat(statementArgCaptor.getAllValues().subList(2, 4)).isEqualTo(Arrays.asList(
					"update Toto set a = ?, b = ?, q = ? where id = ?",
					"update Toto2 set x = ?, y = ?, z = ? where id = ?"));
			assertThat(statementArgCaptor.getAllValues()).hasSize(4);
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
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"update Toto set a = ?, b = ?, q = ? where id = ?",
					"update Toto2 set x = ?, y = ?, z = ? where id = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					// since PreparedStatement.setArray is not watched we don't have q property index (3)
					.newRow(1, 17).add(2, 23).add(4, 1)
					.newRow(1, 29).add(2, 31).add(4, 2)
					.newRow(1, 37).add(2, 41).add(4, 3)
					.newRow(1, 43).add(2, 53).add(4, 4)
					
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
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto2 where id = ?", "delete from Toto where id = ?"));
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
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto2 where id = ?", "delete from Toto where id = ?"));
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
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto2 where id in (?)", "delete from Toto where id in (?)"));
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
																				  "delete from Toto where id in (?, ?, ?)",
																				  "delete from Toto where id in (?)"));
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
			String totoIdAlias = "Toto_id";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoQAlias = "Toto_q";
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
			
			Set<Toto> select = testInstance.select(Arrays.asList(
					new PersistableIdentifier<>(7),
					new PersistableIdentifier<>(13),
					new PersistableIdentifier<>(17),
					new PersistableIdentifier<>(23)
			));
			
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(0)).getColumns()).containsExactlyInAnyOrder(
					"Toto.id as " + totoIdAlias,
							"Toto.a as " + totoAAlias,
							"Toto.b as " + totoBAlias,
							"Toto.q as " + totoQAlias,
							"Toto2.z as " + toto2ZAlias,
							"Toto2.x as " + toto2XAlias,
							"Toto2.y as " + toto2YAlias,
							"Toto2.id as " + toto2IdAlias
			);
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(0)).getFrom()).isEqualTo(
					"Toto inner join Toto2 as Toto2 on Toto.id = Toto2.id where Toto.id in (?, ?, ?)"
			);
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(1)).getColumns()).containsExactlyInAnyOrder(
					"Toto.id as " + totoIdAlias,
							"Toto.a as " + totoAAlias,
							"Toto.b as " + totoBAlias,
							"Toto.q as " + totoQAlias,
							"Toto2.z as " + toto2ZAlias,
							"Toto2.x as " + toto2XAlias,
							"Toto2.y as " + toto2YAlias,
							"Toto2.id as " + toto2IdAlias
			);
			assertThat(new RawQuery(statementArgCaptor.getAllValues().get(1)).getFrom()).isEqualTo(
					"Toto inner join Toto2 as Toto2 on Toto.id = Toto2.id where Toto.id in (?)"
			);
			assertThat(statementArgCaptor.getAllValues()).hasSize(2);
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
		private Set<Integer> q;
		
		private Tata tata;
		
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
		
		public void setQ(Set<Integer> q) {
			this.q = q;
		}
		
		public Set<Integer> getQ() {
			return q;
		}
		
		/**
		 * Method to merge another bean with this one on a part of their attributes.
		 * It has no real purpose, it only exists to fulfill the relational mapping between tables Toto and Toto2 and avoid a NullPointerException
		 * when associating 2 results of RowTransformer
		 * 
		 * @param another a bean coming from the persister
		 */
		void merge(Toto another) {
			this.x = another.x;
			this.y = another.y;
			this.z = another.z;
		}
		
		public Tata getTata() {
			return tata;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", (Object) id.getSurrogate()).add("a", a).add("b", b).add("x", x).add("y", y).add("z", z)
					+ "]";
		}
	}
	
	private static class Tata {
		
		private Integer id;
		
		private String prop1;
		
		public Integer getId() {
			return id;
		}
		
		public String getProp1() {
			return prop1;
		}
	}
}