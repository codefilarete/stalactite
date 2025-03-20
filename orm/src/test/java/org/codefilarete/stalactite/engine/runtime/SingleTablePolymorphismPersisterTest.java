package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.InMemoryCounterIdentifierGenerator;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.PersistentFieldHarvester;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
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
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.tool.trace.MutableLong;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.types.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.stalactite.engine.runtime.SingleTablePolymorphismEntitySelector.DISCRIMINATOR_ALIAS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SingleTablePolymorphismPersisterTest {
	
	private SingleTablePolymorphismPersister<AbstractToto, Identifier<Integer>, ?, String> testInstance;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> statementArgCaptor;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	private ClassMapping<AbstractToto, Identifier<Integer>, ?> totoClassMapping;
	private DefaultDialect dialect;
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
	
	protected ConfiguredRelationalPersister<TotoA, Identifier<Integer>> initMappingTotoA(Table totoTable) {
		PersistentFieldHarvester persistentFieldHarvester = new PersistentFieldHarvester();
		Map<PropertyAccessor<TotoA, Object>, Column<Table, Object>> mappedFields = persistentFieldHarvester.mapFields(TotoA.class, totoTable);
		PropertyAccessor<TotoA, Identifier<Integer>> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarvester.getField("id"));
		persistentFieldHarvester.getColumn(primaryKeyAccessor).primaryKey();
		
		IdentifierInsertionManager<TotoA, Identifier<Integer>> identifierManager = (IdentifierInsertionManager) totoClassMapping.getIdMapping().getIdentifierInsertionManager();
		
		return new SimpleRelationalEntityPersister<>(new ClassMapping<>(TotoA.class,
				totoTable,
				mappedFields,
				primaryKeyAccessor,
				identifierManager), dialect, new ConnectionConfigurationSupport(() -> connection, 3));
	}
	
	protected ConfiguredRelationalPersister<TotoB, Identifier<Integer>> initMappingTotoB(Table totoTable) {
		PersistentFieldHarvester persistentFieldHarvester = new PersistentFieldHarvester();
		Map<PropertyAccessor<TotoB, Object>, Column<Table, Object>> mappedFields = persistentFieldHarvester.mapFields(TotoB.class, totoTable);
		PropertyAccessor<TotoB, Identifier<Integer>> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarvester.getField("id"));
		persistentFieldHarvester.getColumn(primaryKeyAccessor).primaryKey();
		
		IdentifierInsertionManager<TotoB, Identifier<Integer>> identifierManager = (IdentifierInsertionManager) totoClassMapping.getIdMapping().getIdentifierInsertionManager();
		
		return new SimpleRelationalEntityPersister<>(new ClassMapping<>(TotoB.class,
				totoTable,
				mappedFields,
				primaryKeyAccessor,
				identifierManager), dialect, new ConnectionConfigurationSupport(() -> connection, 3));
	}
	
	protected void initMapping() {
		Field fieldId = Reflections.getField(AbstractToto.class, "id");
		Field fieldA = Reflections.getField(AbstractToto.class, "x");
		Field fieldQ = Reflections.getField(AbstractToto.class, "q");
		
		Table totoTable = new Table("Toto");
		Column<Table, Object> idColumn = totoTable.addColumn("id", fieldId.getType()).primaryKey();
		Column<Table, Object> xColumn = totoTable.addColumn("x", fieldA.getType());
		Column<Table, Object> qColumn = totoTable.addColumn("q", fieldQ.getType());
		
		PropertyAccessor<AbstractToto, Identifier<Integer>> identifierAccessor = Accessors.propertyAccessor(fieldId);
		Map<PropertyAccessor<AbstractToto, Object>, Column<Table, Object>> totoPropertyMapping = Maps.forHashMap(
				(Class<PropertyAccessor<AbstractToto, Object>>) (Class) PropertyAccessor.class, (Class<Column<Table, Object>>) (Class) Column.class)
				.add((PropertyAccessor) identifierAccessor, idColumn)
				.add(Accessors.propertyAccessor(fieldA), xColumn)
				.add(Accessors.propertyAccessor(fieldQ), qColumn);
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		
		BeforeInsertIdentifierManager<AbstractToto, Identifier<Integer>> beforeInsertIdentifierManager = new BeforeInsertIdentifierManager<>(
				new AccessorWrapperIdAccessor<>(identifierAccessor),
				() -> new PersistableIdentifier<>(identifierGenerator.next()),
				(Class<Identifier<Integer>>) (Class) Identifier.class);
		totoClassMapping = new ClassMapping<>(AbstractToto.class,
				totoTable,
				totoPropertyMapping,
				identifierAccessor,
				beforeInsertIdentifierManager);
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Identifier.class, "int");
		
		dialect = new DefaultDialect(simpleTypeMapping);
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
	
	protected <T extends Table<T>> void initTest() throws SQLException {
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
		
		ConfiguredRelationalPersister<AbstractToto, Identifier<Integer>> mainPersister = new SimpleRelationalEntityPersister<>(totoClassMapping, dialect, new ConnectionConfigurationSupport(() -> connection, 3));
		ConfiguredRelationalPersister<TotoA, Identifier<Integer>> totoAIdentifierConfiguredPersister = initMappingTotoA(mainPersister.getMainTable());
		ConfiguredRelationalPersister<TotoB, Identifier<Integer>> totoBIdentifierConfiguredPersister = initMappingTotoB(mainPersister.getMainTable());
		Map<Class<? extends AbstractToto>, ConfiguredPersister<? extends AbstractToto, Identifier<Integer>>> subclasses = Maps
				.forHashMap((Class<Class<? extends AbstractToto>>) null, (Class<ConfiguredPersister<? extends AbstractToto, Identifier<Integer>>>) null)
				.add(TotoA.class, totoAIdentifierConfiguredPersister)
				.add(TotoB.class, totoBIdentifierConfiguredPersister);
		// We specify discriminator as an Integer because it's the same type as other tested columns and simplify data capture and comparison
		Column<?, Integer> dtype = totoClassMapping.getTargetTable().addColumn("DTYPE", Integer.class);
		SingleTablePolymorphism<AbstractToto, Integer> polymorphismPolicy = new SingleTablePolymorphism<>(dtype.getName(), dtype.getJavaType());
		polymorphismPolicy.addSubClass(new EmptySubEntityMappingConfiguration<>(TotoA.class), 100);
		polymorphismPolicy.addSubClass(new EmptySubEntityMappingConfiguration<>(TotoB.class), 200);
		
		testInstance = new SingleTablePolymorphismPersister<>(
				mainPersister,
				(Map) subclasses,
				new ConnectionConfigurationSupport(new CurrentThreadConnectionProvider(dataSource), 3).getConnectionProvider(),
				dialect,
				dtype,
				polymorphismPolicy);
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
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1, 1 }, new long[] { 1, 1 }));
			testInstance.insert(Arrays.asList(
					new TotoA(17, 23),
					new TotoA(29, 31),
					new TotoB(37, 41),
					new TotoB(43, 53)
			));
			
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			// since all columns are of same type (Integer) we can capture them all in one statement, else (different types)
			// it's much more difficult
			verify(preparedStatement, times(16)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"insert into Toto(a, id, x, q, DTYPE) values (?, ?, ?, ?, ?)",
					"insert into Toto(b, id, x, q, DTYPE) values (?, ?, ?, ?, ?)"
			));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 23).add(2, 1).add(3, 17).add(5, 100)
					.newRow(1, 31).add(2, 2).add(3, 29).add(5, 100)
					.newRow(1, 41).add(2, 3).add(3, 37).add(5, 200)
					.newRow(1, 53).add(2, 4).add(3, 43).add(5, 200);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void update() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto_id";
			String totoXAlias = "Toto_x";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoDTYPEAlias = "Toto_DTYPE";
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 2).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 3).add(totoDTYPEAlias, 200),
							Maps.asMap(totoIdAlias, 4).add(totoDTYPEAlias, 200)
					)),
					// second result is for TotoA entities read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(totoXAlias, 17).add(totoAAlias, 23).add(totoBAlias, null).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 2).add(totoXAlias, 29).add(totoAAlias, 31).add(totoBAlias, null).add(totoDTYPEAlias, 100)
					)),
					// second result is for TotoB entities read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 3).add(totoXAlias, 37).add(totoAAlias, null).add(totoBAlias, 41).add(totoDTYPEAlias, 200),
							Maps.asMap(totoIdAlias, 4).add(totoXAlias, 43).add(totoAAlias, null).add(totoBAlias, 53).add(totoDTYPEAlias, 200)
					))
			);
			
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 2 }, new long[] { 2 }));
			// Note that since we didn't Fluent API to build our test instance, the default process that computes
			// differences between memory and DB is not used, then we'll use UpdateExecutor that roughly update all
			testInstance.update(Arrays.asList(
					new TotoA(1, 17, 123),
					new TotoA(2, 29, 131),
					new TotoB(3, 37, 141),
					new TotoB(4, 43, 153)
			));
			
			// 3 queries because we select ids first in a separate query, then we select 4 entities which is made
			// through 2 queries, to be spread over in(..) operator that has a maximum size of 3
			verify(preparedStatement, times(3)).executeQuery();
			verify(preparedStatement, times(20)).setInt(indexCaptor.capture(), valueCaptor.capture());
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + totoIdAlias
							+ ", Toto.DTYPE as " + totoDTYPEAlias
							+ " from Toto where Toto.id in (?, ?, ?, ?)",
					"select Toto.a as " + totoAAlias
							+ ", Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ " from Toto where Toto.id in (?, ?)",
					"select Toto.b as " + totoBAlias
							+ ", Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ " from Toto where Toto.id in (?, ?)",
					"update Toto set a = ?, x = ?, q = ? where id = ?",
					"update Toto set b = ?, x = ?, q = ? where id = ?"));
			// captured setInt(..) is made of ids
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2).add(3, 3).add(4, 4)
					// this one if for the lonely last select
					.newRow(1, 1).add(2, 2)
						// this one if for the lonely last select
					.newRow(1, 3).add(2, 4)
					.newRow(1, 123).add(2, 17).add(4, 1)
					.newRow(1, 131).add(2, 29).add(4, 2)
					.newRow(1, 141).add(2, 37).add(4, 3)
					.newRow(1, 153).add(2, 43).add(4, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void updateById() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 2 }, new long[] { 2 }));
			testInstance.updateById(Arrays.asList(
					new TotoA(1, 17, 123),
					new TotoA(2, 29, 131),
					new TotoB(3, 37, 141),
					new TotoB(4, 43, 153)
			));
			
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"update Toto set a = ?, x = ?, q = ? where id = ?",
					"update Toto set b = ?, x = ?, q = ? where id = ?"
			));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 123).add(2, 17).add(4, 1)
					.newRow(1, 131).add(2, 29).add(4, 2)
					.newRow(1, 141).add(2, 37).add(4, 3)
					.newRow(1, 153).add(2, 43).add(4, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void delete() throws SQLException {
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1 }));
			testInstance.delete(Arrays.asList(new TotoA(7, 17, 23)));
			
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
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 2 }, new long[] { 2 }));
			testInstance.delete(Arrays.asList(
					new TotoA(1, 17, 23),
					new TotoA(2, 29, 31),
					new TotoB(3, 37, 41),
					new TotoB(4, 43, 53)
			));
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"delete from Toto where id = ?",
					"delete from Toto where id = ?"));
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
					new TotoA(7, 17, 23)
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
			expectedRowCountForUpdate.set(2L);
			testInstance.deleteById(Arrays.asList(
					new TotoA(1, 17, 23),
					new TotoA(2, 29, 31),
					new TotoB(3, 37, 41),
					new TotoB(4, 43, 53)
			));
			// 4 statements because in operator is bounded to 3 values (see testInstance creation)
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where id in (?, ?)",
					"delete from Toto where id in (?, ?)"));
			verify(preparedStatement, times(2)).executeLargeUpdate();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2)
					.newRow(1, 3).add(2, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void select() throws SQLException {
			String totoIdAlias = "Toto_id";
			String totoXAlias = "Toto_x";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoDTYPEAlias = "Toto_DTYPE";
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 2).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 3).add(totoDTYPEAlias, 200),
							Maps.asMap(totoIdAlias, 4).add(totoDTYPEAlias, 200)
					)),
					// second result is for TotoA entities read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(totoXAlias, 17).add(totoAAlias, 23).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 2).add(totoXAlias, 29).add(totoAAlias, 31).add(totoDTYPEAlias, 100)
					)),
					// third result is for TotoB entities read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 3).add(totoXAlias, 37).add(totoBAlias, 41).add(totoDTYPEAlias, 200),
							Maps.asMap(totoIdAlias, 4).add(totoXAlias, 43).add(totoBAlias, 53).add(totoDTYPEAlias, 200)
					))
			);
			
			Set<AbstractToto> select = testInstance.select(Arrays.asList(
					new PersistedIdentifier<>(1),
					new PersistedIdentifier<>(2),
					new PersistedIdentifier<>(3),
					new PersistedIdentifier<>(4)
			));
			
			// 3 queries because we select ids first in a separate query, then we select 4 entities which is made
			// through 2 queries, to be spread over in(..) operator that has a maximum size of 3
			verify(preparedStatement, times(3)).executeQuery();
			verify(preparedStatement, times(8)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + totoIdAlias
							+ ", Toto.DTYPE as " + totoDTYPEAlias
							+ " from Toto where Toto.id in (?, ?, ?, ?)",
					"select Toto.a as " + totoAAlias
							+ ", Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ " from Toto where Toto.id in (?, ?)",
					"select Toto.b as " + totoBAlias
							+ ", Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ " from Toto where Toto.id in (?, ?)"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2).add(3, 3).add(4, 4)
					// this one if for the selection of A instances
					.newRow(1, 1).add(2, 2)
					// this one if for the selection of B instances
					.newRow(1, 3).add(2, 4);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(select).usingRecursiveFieldByFieldElementComparator()
					.containsExactlyInAnyOrder(
							new TotoA(1, 17, 23),
							new TotoA(2, 29, 31),
							new TotoB(3, 37, 41),
							new TotoB(4, 43, 53));
		}
		
		@Test
		void selectWhere() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto_id";
			String totoXAlias = "Toto_x";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoDTYPEAlias = "Toto_DTYPE";
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(totoXAlias, 17).add(totoAAlias, 23).add(totoBAlias, null).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 2).add(totoXAlias, 29).add(totoAAlias, 31).add(totoBAlias, null).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 3).add(totoXAlias, 37).add(totoAAlias, null).add(totoBAlias, 41).add(totoDTYPEAlias, 200),
							Maps.asMap(totoIdAlias, 4).add(totoXAlias, 43).add(totoAAlias, null).add(totoBAlias, 53).add(totoDTYPEAlias, 200)
					))
			);
			
			ExecutableEntityQueryCriteria<AbstractToto, ?> totoExecutableEntityQueryCriteria = testInstance.selectWhere(AbstractToto::getX, Operators.eq(42));
			Set<AbstractToto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select "
							+ "Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.DTYPE as " + totoDTYPEAlias
							+ " from Toto"
							+ " where Toto.x = ?"
			));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 42);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(select).usingRecursiveFieldByFieldElementComparator()
					.containsExactlyInAnyOrder(
							new TotoA(1, 17, 23),
							new TotoA(2, 29, 31),
							new TotoB(3, 37, 41),
							new TotoB(4, 43, 53));
		}
		
		@Test
		void selectWhere_orderByOnNonCollectionProperty_orderByIsAddedToSQL() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto_id";
			String totoXAlias = "Toto_x";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoDTYPEAlias = "Toto_DTYPE";
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(totoXAlias, 17).add(totoAAlias, 23).add(totoBAlias, null).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 2).add(totoXAlias, 29).add(totoAAlias, 31).add(totoBAlias, null).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 3).add(totoXAlias, 37).add(totoAAlias, null).add(totoBAlias, 41).add(totoDTYPEAlias, 200),
							Maps.asMap(totoIdAlias, 4).add(totoXAlias, 43).add(totoAAlias, null).add(totoBAlias, 53).add(totoDTYPEAlias, 200)
					))
			);
			
			ExecutableEntityQueryCriteria<AbstractToto, ?> totoExecutableEntityQueryCriteria = testInstance.selectWhere(AbstractToto::getX, Operators.eq(42))
					.orderBy(AbstractToto::getX);
			Set<AbstractToto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select "
							+ "Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ ", Toto.a as " + totoAAlias
							+ ", Toto.b as " + totoBAlias
							+ ", Toto.DTYPE as " + totoDTYPEAlias
							+ " from Toto"
							+ " where Toto.x = ?"
							+ " order by Toto.x asc"
			));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 42);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(select).usingRecursiveFieldByFieldElementComparator()
					.containsExactlyInAnyOrder(
							new TotoA(1, 17, 23),
							new TotoA(2, 29, 31),
							new TotoB(3, 37, 41),
							new TotoB(4, 43, 53));
		}
		
		@Test
		void selectWhere_orderByOnCollectionProperty_throwsException() {
			assertThatCode(() -> testInstance.selectWhere(AbstractToto::getX, Operators.eq(42)).orderBy(AbstractToto::getQ))
					.hasMessage("OrderBy clause on a Collection property is unsupported due to eventual inconsistency with Collection nature :"
							+ " o.c.s.e.r.SingleTablePolymorphismPersisterTest$AbstractToto::getQ");
		}
		
		@Test
		void selectWhere_collectionCriteria() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String totoIdAlias = "Toto_id";
			String totoXAlias = "Toto_x";
			String totoAAlias = "Toto_a";
			String totoBAlias = "Toto_b";
			String totoDTYPEAlias = "Toto_DTYPE";
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(DISCRIMINATOR_ALIAS, 100),
							Maps.asMap(totoIdAlias, 2).add(DISCRIMINATOR_ALIAS, 100),
							Maps.asMap(totoIdAlias, 3).add(DISCRIMINATOR_ALIAS, 200),
							Maps.asMap(totoIdAlias, 4).add(DISCRIMINATOR_ALIAS, 200)
					)),
					// second result is for TotoA entities read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 1).add(totoXAlias, 17).add(totoAAlias, 23).add(totoBAlias, null).add(totoDTYPEAlias, 100),
							Maps.asMap(totoIdAlias, 2).add(totoXAlias, 29).add(totoAAlias, 31).add(totoBAlias, null).add(totoDTYPEAlias, 100)
					)),
					// third result is for TotoB entities read
					new InMemoryResultSet(Arrays.asList(
							Maps.asMap(totoIdAlias, 3).add(totoXAlias, 37).add(totoAAlias, null).add(totoBAlias, 41).add(totoDTYPEAlias, 200),
							Maps.asMap(totoIdAlias, 4).add(totoXAlias, 43).add(totoAAlias, null).add(totoBAlias, 53).add(totoDTYPEAlias, 200)
					))
			);
			
			ExecutableEntityQueryCriteria<AbstractToto, ?> totoExecutableEntityQueryCriteria = testInstance.selectWhere(AbstractToto::getQ, Operators.eq(Arrays.asHashSet(42)));
			Set<AbstractToto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(3)).executeQuery();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + totoIdAlias
							+ ", Toto.DTYPE as " + DISCRIMINATOR_ALIAS
							+ " from Toto where Toto.q = ?",
					"select Toto.a as " + totoAAlias
							+ ", Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ " from Toto where Toto.id in (?, ?)",
					"select Toto.b as " + totoBAlias
							+ ", Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ " from Toto where Toto.id in (?, ?)"));
			// Here we don't test the "42" row of the first query because it requires to listen to PreparedStatement.setArray(..) whereas all this
			// test class is bound to PreparedStatement.setInt(..) and Integer type.
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 1);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(select).usingRecursiveFieldByFieldElementComparator()
					.containsExactlyInAnyOrder(
							new TotoA(1, 17, 23),
							new TotoA(2, 29, 31),
							new TotoB(3, 37, 41),
							new TotoB(4, 43, 53));
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
		
		@Test
		void selectProjectionWhere() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			ResultSet resultSet = new InMemoryResultSet(Arrays.asList(
					Maps.asMap("count", 42L)
			));
			when(preparedStatement.executeQuery()).thenAnswer((Answer<ResultSet>) invocation -> resultSet);
			
			Count count = Operators.count(new SelectableString<>("id", Integer.class));
			ExecutableProjectionQuery<AbstractToto, ?> totoRelationalExecutableEntityQuery = testInstance.selectProjectionWhere(select ->  {
				select.clear();
				select.add(count, "count");
			}, AbstractToto::getX, Operators.eq(77));
			long countValue = totoRelationalExecutableEntityQuery.execute(new Accumulator<Function<Selectable<Long>, Long>, MutableLong, Long>() {
				@Override
				public Supplier<MutableLong> supplier() {
					return MutableLong::new;
				}
				
				@Override
				public BiConsumer<MutableLong, Function<Selectable<Long>, Long>> aggregator() {
					return (modifiableInt, selectableObjectFunction) -> {
						Long apply = selectableObjectFunction.apply(count);
						modifiableInt.reset(apply);
					};
				}
				
				@Override
				public Function<MutableLong, Long> finisher() {
					return MutableLong::getValue;
				}
			});
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select count(id) as count from Toto where Toto.x = ?"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().newRow(1, 77);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(countValue).isEqualTo(42);
		}
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
	
	private static abstract class AbstractToto implements Identified<Integer> {
		protected Identifier<Integer> id;
		protected Integer x;
		private Set<Integer> q;
		
		public AbstractToto() {
		}
		
		public AbstractToto(int id, Integer x) {
			this.id = new PersistableIdentifier<>(id);
			this.x = x;
		}
		
		public AbstractToto(Integer x) {
			this.x = x;
		}
		
		@Override
		public Identifier<Integer> getId() {
			return id;
		}
		
		public Integer getX() {
			return x;
		}
		
		public Set<Integer> getQ() {
			return q;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", id == null ? "null" : id.getSurrogate()).add("x", x)
					+ "]";
		}
	}
	
	static class TotoA extends AbstractToto {
		
		private Integer a;
		
		TotoA() {
		}
		
		TotoA(int id, Integer x, Integer a) {
			super(id, x);
			this.a = a;
		}
		
		TotoA(Integer x, Integer a) {
			super(x);
			this.a = a;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", id == null ? "null" : id.getSurrogate())
						.add("x", x)
						.add("a", a)
					+ "]";
		}
	}
	
	static class TotoB extends AbstractToto {
		
		private Integer b;
		
		public TotoB() {
		}
		
		TotoB(int id, Integer x, Integer b) {
			super(id, x);
			this.b = b;
		}
		
		TotoB(Integer x, Integer b) {
			super(x);
			this.b = b;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", id == null ? "null" : id.getSurrogate())
					.add("x", x)
					.add("b", b)
					+ "]";
		}
	}
}