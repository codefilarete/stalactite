package org.codefilarete.stalactite.engine.runtime.tableperclass;

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

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.InMemoryCounterIdentifierGenerator;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableBeanPropertyQueryMapper;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Engine;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.EmptySubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
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
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Sequence;
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
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TablePerClassPolymorphismPersisterTest {
	
	private TablePerClassPolymorphismPersister<AbstractToto, Identifier<Integer>, ?> testInstance;
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
	
	protected ConfiguredRelationalPersister<TotoA, Identifier<Integer>> initMappingTotoA(Table table) {
		PersistentFieldHarvester persistentFieldHarvester = new PersistentFieldHarvester();
		Map<PropertyAccessor<TotoA, ?>, Column<Table, ?>> mappedFields = persistentFieldHarvester.mapFields(TotoA.class, table);
		PropertyAccessor<TotoA, Identifier<Integer>> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarvester.getField("id"));
		persistentFieldHarvester.getColumn(primaryKeyAccessor).primaryKey();
		
		IdentifierInsertionManager<TotoA, Identifier<Integer>> identifierManager = (IdentifierInsertionManager) totoClassMapping.getIdMapping().getIdentifierInsertionManager();
		
		return new SimpleRelationalEntityPersister<>(new ClassMapping<>(TotoA.class,
				table,
				mappedFields,
				primaryKeyAccessor,
				identifierManager), dialect, new ConnectionConfigurationSupport(() -> connection, 3));
	}
	
	protected ConfiguredRelationalPersister<TotoB, Identifier<Integer>> initMappingTotoB(Table table) {
		PersistentFieldHarvester persistentFieldHarvester = new PersistentFieldHarvester();
		Map<PropertyAccessor<TotoB, ?>, Column<Table, ?>> mappedFields = persistentFieldHarvester.mapFields(TotoB.class, table);
		PropertyAccessor<TotoB, Identifier<Integer>> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarvester.getField("id"));
		persistentFieldHarvester.getColumn(primaryKeyAccessor).primaryKey();
		
		IdentifierInsertionManager<TotoB, Identifier<Integer>> identifierManager = (IdentifierInsertionManager) totoClassMapping.getIdMapping().getIdentifierInsertionManager();
		
		return new SimpleRelationalEntityPersister<>(new ClassMapping<>(TotoB.class,
				table,
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
				statement.setInt(valueIndex, (Integer) value.getDelegate());
			}
		});
		// Registering a binder of Set for the AbstractToto.q property
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
		Table<?> totoATable = new Table<>("TotoA");
		Table<?> totoBTable = new Table<>("TotoB");
		ConfiguredRelationalPersister<TotoA, Identifier<Integer>> totoAIdentifierConfiguredPersister = initMappingTotoA(totoATable);
		ConfiguredRelationalPersister<TotoB, Identifier<Integer>> totoBIdentifierConfiguredPersister = initMappingTotoB(totoBTable);
		// We keep order of subclasses to get steady unit tests, code has also been adapted to keep it
		Map<Class<? extends AbstractToto>, ConfiguredPersister<? extends AbstractToto, Identifier<Integer>>> subclasses = new KeepOrderMap<>();
		subclasses.put(TotoA.class, totoAIdentifierConfiguredPersister);
		subclasses.put(TotoB.class, totoBIdentifierConfiguredPersister);
		// We specify discriminator as an Integer because it's the same type as other tested columns and simplify data capture and comparison
		TablePerClassPolymorphism<AbstractToto> polymorphismPolicy = new TablePerClassPolymorphism<>();
		polymorphismPolicy.addSubClass(new EmptySubEntityMappingConfiguration<>(TotoA.class), totoATable);
		polymorphismPolicy.addSubClass(new EmptySubEntityMappingConfiguration<>(TotoB.class), totoBTable);
		
		testInstance = new TablePerClassPolymorphismPersister<>(
				mainPersister,
				(Map) subclasses,
				new ConnectionConfigurationSupport(new CurrentThreadConnectionProvider(dataSource), 3).getConnectionProvider(),
				dialect);
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
			verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"insert into TotoA(a, id, x, q) values (?, ?, ?, ?)",
					"insert into TotoB(b, id, x, q) values (?, ?, ?, ?)"
			));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 23).add(2, 1).add(3, 17)
					.newRow(1, 31).add(2, 2).add(3, 29)
					.newRow(1, 41).add(2, 3).add(3, 37)
					.newRow(1, 53).add(2, 4).add(3, 43);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void update() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String idAlias = "Toto_id";
			String xAlias = "Toto_x";
			String totoAIdAlias = "TotoA_id";
			String totoBIdAlias = "TotoB_id";
			String totoAXAlias = "TotoA_x";
			String totoBXAlias = "TotoB_x";
			String totoAQAlias = "TotoA_q";
			String totoBQAlias = "TotoB_q";
			String totoAAlias = "TotoA_a";
			String totoBAlias = "TotoB_b";
			String totoDTYPEAlias = "Toto_DISCRIMINATOR";
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.forHashMap(String.class, Object.class).add(idAlias, 1).add(totoDTYPEAlias, "TotoA").add(xAlias, 17)
									.add(totoAIdAlias, 1).add(totoAXAlias, 17).add(totoAAlias, 23)
									.add(totoBIdAlias, null).add(totoBAlias, null).add(totoBXAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 2).add(totoDTYPEAlias, "TotoA").add(xAlias, 29)
									.add(totoAIdAlias, 2).add(totoAXAlias, 29).add(totoAAlias, 31)
									.add(totoBIdAlias, null).add(totoBAlias, null).add(totoBXAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 3).add(totoDTYPEAlias, "TotoB").add(xAlias, 37)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAlias, null)
									.add(totoBIdAlias, 3).add(totoBAlias, 41).add(totoBXAlias, 37),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 4).add(totoDTYPEAlias, "TotoB").add(xAlias, 43)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAlias, null)
									.add(totoBIdAlias, 4).add(totoBAlias, 53).add(totoBXAlias, 43)
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
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(16)).setInt(indexCaptor.capture(), valueCaptor.capture());
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + idAlias
							+ ", Toto.x as " + xAlias
							+ ", Toto.q as Toto_q"
							+ ", Toto.DISCRIMINATOR as Toto_DISCRIMINATOR"
							+ ", TotoA.a as " + totoAAlias
							+ ", TotoA.id as " + totoAIdAlias
							+ ", TotoA.x as " + totoAXAlias
							+ ", TotoA.q as " + totoAQAlias
							+ ", TotoB.b as " + totoBAlias
							+ ", TotoB.id as " + totoBIdAlias
							+ ", TotoB.x as " + totoBXAlias
							+ ", TotoB.q as " + totoBQAlias
							+ " from ("
								+ "select TotoA.id as id,"
								+ " TotoA.x as x,"
								+ " TotoA.q as q,"
								+ " 'TotoA' as DISCRIMINATOR"
								+ " from TotoA"
								+ " union all"
								+ " select TotoB.id as id,"
								+ " TotoB.x as x,"
								+ " TotoB.q as q,"
								+ " 'TotoB' as DISCRIMINATOR from TotoB)"
							+ " as Toto"
							+ " left outer join TotoA as TotoA on Toto.id = TotoA.id"
							+ " left outer join TotoB as TotoB on Toto.id = TotoB.id"
							+ " where Toto.id in (?, ?, ?, ?)",
					"update TotoA set a = ?, x = ?, q = ? where id = ?",
					"update TotoB set b = ?, x = ?, q = ? where id = ?"));
			// captured setInt(..) is made of ids
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2).add(3, 3).add(4, 4)
					// since PreparedStatement.setArray is not watched we don't have q property index (3)
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
					"update TotoA set a = ?, x = ?, q = ? where id = ?",
					"update TotoB set b = ?, x = ?, q = ? where id = ?"
			));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					// since PreparedStatement.setArray is not watched we don't have q property index (3)
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
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from TotoA where id = ?"));
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
					"delete from TotoA where id = ?",
					"delete from TotoB where id = ?"));
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
			
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from TotoA where id in (?)"));
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
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"delete from TotoA where id in (?, ?)",
					"delete from TotoB where id in (?, ?)"));
			verify(preparedStatement, times(2)).executeLargeUpdate();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2)
					.newRow(1, 3).add(2, 4);
			assertCapturedPairsEqual(expectedPairs);
		}
		
		@Test
		void select() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String idAlias = "Toto_id";
			String xAlias = "Toto_x";
			String qAlias = "Toto_q";
			String totoAIdAlias = "TotoA_id";
			String totoAAAlias = "TotoA_a";
			String totoAXAlias = "TotoA_x";
			String totoBIdAlias = "TotoB_id";
			String totoBBAlias = "TotoB_b";
			String totoBXAlias = "TotoB_x";
			String totoDTYPEAlias = "Toto_" + TablePerClassPolymorphismEntityFinder.DISCRIMINATOR_ALIAS;
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.forHashMap(String.class, Object.class).add(idAlias, 1).add(totoDTYPEAlias, "TotoA").add(xAlias, 17)
									.add(totoAIdAlias, 1).add(totoAXAlias, 17).add(totoAAAlias, 23)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 2).add(totoDTYPEAlias, "TotoA").add(xAlias, 29)
									.add(totoAIdAlias, 2).add(totoAXAlias, 29).add(totoAAAlias, 31)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 3).add(totoDTYPEAlias, "TotoB").add(xAlias, 37)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAAlias, null)
									.add(totoBIdAlias, 3).add(totoBXAlias, 37).add(totoBBAlias, 41),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 4).add(totoDTYPEAlias, "TotoB").add(xAlias, 43)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAAlias, null)
									.add(totoBIdAlias, 4).add(totoBXAlias, 43).add(totoBBAlias, 53)
					))
			);
			
			Set<AbstractToto> select = testInstance.select(Arrays.asList(
					new PersistedIdentifier<>(1),
					new PersistedIdentifier<>(2),
					new PersistedIdentifier<>(3),
					new PersistedIdentifier<>(4)
			));
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select"
							+ " Toto.id as " + idAlias
							+ ", Toto.x as " + xAlias
							+ ", Toto.q as " + qAlias
							+ ", Toto.DISCRIMINATOR as " + totoDTYPEAlias
							+ ", TotoA.a as " + totoAAAlias
							+ ", TotoA.id as " + totoAIdAlias
							+ ", TotoA.x as " + totoAXAlias
							+ ", TotoA.q as " + "TotoA_q"
							+ ", TotoB.b as " + totoBBAlias
							+ ", TotoB.id as " + totoBIdAlias
							+ ", TotoB.x as " + totoBXAlias
							+ ", TotoB.q as " + "TotoB_q"
							+ " from ("
							+ "select TotoA.id as id,"
							+ " TotoA.x as x,"
							+ " TotoA.q as q,"
							+ " 'TotoA' as DISCRIMINATOR from TotoA"
							+ " union all "
							+ "select TotoB.id as id,"
							+ " TotoB.x as x,"
							+ " TotoB.q as q,"
							+ " 'TotoB' as DISCRIMINATOR from TotoB)"
							+ " as Toto left outer join TotoA as TotoA on Toto.id = TotoA.id left outer join TotoB as TotoB on Toto.id = TotoB.id where Toto.id in (?, ?, ?, ?)"));
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2).add(3, 3).add(4, 4);
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
			String idAlias = "Toto_id";
			String xAlias = "Toto_x";
			String qAlias = "Toto_q";
			String totoAIdAlias = "TotoA_id";
			String totoAAAlias = "TotoA_a";
			String totoAXAlias = "TotoA_x";
			String totoBIdAlias = "TotoB_id";
			String totoBBAlias = "TotoB_b";
			String totoBXAlias = "TotoB_x";
			String totoDTYPEAlias = "Toto_" + TablePerClassPolymorphismEntityFinder.DISCRIMINATOR_ALIAS;
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.forHashMap(String.class, Object.class).add(idAlias, 1).add(totoDTYPEAlias, "TotoA").add(xAlias, 17)
									.add(totoAIdAlias, 1).add(totoAXAlias, 17).add(totoAAAlias, 23)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 2).add(totoDTYPEAlias, "TotoA").add(xAlias, 29)
									.add(totoAIdAlias, 2).add(totoAXAlias, 29).add(totoAAAlias, 31)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 3).add(totoDTYPEAlias, "TotoB").add(xAlias, 37)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAAlias, null)
									.add(totoBIdAlias, 3).add(totoBXAlias, 37).add(totoBBAlias, 41),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 4).add(totoDTYPEAlias, "TotoB").add(xAlias, 43)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAAlias, null)
									.add(totoBIdAlias, 4).add(totoBXAlias, 43).add(totoBBAlias, 53)
					))
			);
			
			ExecutableEntityQueryCriteria<AbstractToto, ?> totoExecutableEntityQueryCriteria = testInstance.selectWhere(AbstractToto::getX, Operators.eq(42));
			Set<AbstractToto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + idAlias
							+ ", Toto.x as " + xAlias
							+ ", Toto.q as " + qAlias
							+ ", Toto.DISCRIMINATOR as " + totoDTYPEAlias
							+ ", TotoA.a as " + totoAAAlias
							+ ", TotoA.id as " + totoAIdAlias
							+ ", TotoA.x as " + totoAXAlias
							+ ", TotoA.q as " + "TotoA_q"
							+ ", TotoB.b as " + totoBBAlias
							+ ", TotoB.id as " + totoBIdAlias
							+ ", TotoB.x as " + totoBXAlias
							+ ", TotoB.q as " + "TotoB_q"
							+ " from ("
							+ "select TotoA.id as id,"
							+ " TotoA.x as x,"
							+ " TotoA.q as q,"
							+ " 'TotoA' as DISCRIMINATOR from TotoA"
							+ " union all "
							+ "select TotoB.id as id,"
							+ " TotoB.x as x,"
							+ " TotoB.q as q,"
							+ " 'TotoB' as DISCRIMINATOR from TotoB)"
							+ " as Toto left outer join TotoA as TotoA on Toto.id = TotoA.id left outer join TotoB as TotoB on Toto.id = TotoB.id where Toto.x = ?"));
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
			String idAlias = "Toto_id";
			String xAlias = "Toto_x";
			String qAlias = "Toto_q";
			String totoAIdAlias = "TotoA_id";
			String totoAAAlias = "TotoA_a";
			String totoAXAlias = "TotoA_x";
			String totoBIdAlias = "TotoB_id";
			String totoBBAlias = "TotoB_b";
			String totoBXAlias = "TotoB_x";
			String totoDTYPEAlias = "Toto_" + TablePerClassPolymorphismEntityFinder.DISCRIMINATOR_ALIAS;
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.forHashMap(String.class, Object.class).add(idAlias, 1).add(totoDTYPEAlias, "TotoA").add(xAlias, 17)
									.add(totoAIdAlias, 1).add(totoAXAlias, 17).add(totoAAAlias, 23)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 2).add(totoDTYPEAlias, "TotoA").add(xAlias, 29)
									.add(totoAIdAlias, 2).add(totoAXAlias, 29).add(totoAAAlias, 31)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 3).add(totoDTYPEAlias, "TotoB").add(xAlias, 37)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAAlias, null)
									.add(totoBIdAlias, 3).add(totoBXAlias, 37).add(totoBBAlias, 41),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 4).add(totoDTYPEAlias, "TotoB").add(xAlias, 43)
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAAlias, null)
									.add(totoBIdAlias, 4).add(totoBXAlias, 43).add(totoBBAlias, 53)
					))
			);
			
			ExecutableEntityQueryCriteria<AbstractToto, ?> totoExecutableEntityQueryCriteria = testInstance.selectWhere(AbstractToto::getX, Operators.eq(42))
					.orderBy(AbstractToto::getX);
			Set<AbstractToto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(1)).executeQuery();
			verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList(
					"select Toto.id as " + idAlias
							+ ", Toto.x as " + xAlias
							+ ", Toto.q as " + qAlias
							+ ", Toto.DISCRIMINATOR as " + totoDTYPEAlias
							+ ", TotoA.a as " + totoAAAlias
							+ ", TotoA.id as " + totoAIdAlias
							+ ", TotoA.x as " + totoAXAlias
							+ ", TotoA.q as " + "TotoA_q"
							+ ", TotoB.b as " + totoBBAlias
							+ ", TotoB.id as " + totoBIdAlias
							+ ", TotoB.x as " + totoBXAlias
							+ ", TotoB.q as " + "TotoB_q"
							+ " from ("
							+ "select TotoA.id as id,"
							+ " TotoA.x as x,"
							+ " TotoA.q as q,"
							+ " 'TotoA' as DISCRIMINATOR from TotoA"
							+ " union all "
							+ "select TotoB.id as id,"
							+ " TotoB.x as x,"
							+ " TotoB.q as q,"
							+ " 'TotoB' as DISCRIMINATOR from TotoB)"
							+ " as Toto left outer join TotoA as TotoA on Toto.id = TotoA.id left outer join TotoB as TotoB on Toto.id = TotoB.id where Toto.x = ?"
							+ " order by Toto.x asc"));
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
							+ " o.c.s.e.r.t.TablePerClassPolymorphismPersisterTest$AbstractToto::getQ");
		}
		
		@Test
		void selectWhere_collectionCriteria() throws SQLException {
			// mocking executeQuery not to return null because select method will use the in-memory ResultSet
			String idAlias = "id";
			String xAlias = "x";
			String qAlias = "q";
			String totoAIdAlias = "TotoA_id";
			String totoBIdAlias = "TotoB_id";
			String totoAXAlias = "TotoA_x";
			String totoBXAlias = "TotoB_x";
			String totoAQAlias = "TotoA_q";
			String totoBQAlias = "TotoB_q";
			String totoAAlias = "TotoA_a";
			String totoBAlias = "TotoB_b";
			String totoDTYPEAlias = TablePerClassPolymorphismEntityFinder.DISCRIMINATOR_ALIAS;
			String totoIdAlias = "Toto_id";
			String totoXAlias = "Toto_x";
			String totoDiscriminator = "Toto_DISCRIMINATOR";
			when(preparedStatement.executeQuery()).thenReturn(
					// first result if for id read
					new InMemoryResultSet(Arrays.asList(
							Maps.forHashMap(String.class, Object.class).add(idAlias, 1).add(totoDTYPEAlias, "TotoA").add(totoAXAlias, 17).add(totoAAlias, 23).add(totoBXAlias, null).add(totoBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 2).add(totoDTYPEAlias, "TotoA").add(totoAXAlias, 29).add(totoAAlias, 31).add(totoBXAlias, null).add(totoBAlias, null),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 3).add(totoDTYPEAlias, "TotoB").add(totoAXAlias, null).add(totoAAlias, null).add(totoBXAlias, 37).add(totoBAlias, 41),
							Maps.forHashMap(String.class, Object.class).add(idAlias, 4).add(totoDTYPEAlias, "TotoB").add(totoAXAlias, null).add(totoAAlias, null).add(totoBXAlias, 43).add(totoBAlias, 53)
					)),
					new InMemoryResultSet(Arrays.asList(
							Maps.forHashMap(String.class, Object.class)
									.add(totoIdAlias, 1).add(totoXAlias, 17).add(totoDiscriminator, "TotoA")
									.add(totoAIdAlias, 1).add(totoAXAlias, 17).add(totoAAlias, 23)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBAlias, null),
							Maps.forHashMap(String.class, Object.class)
									.add(totoIdAlias, 2).add(totoXAlias, 29).add(totoDiscriminator, "TotoA")
									.add(totoAIdAlias, 2).add(totoAXAlias, 29).add(totoAAlias, 31)
									.add(totoBIdAlias, null).add(totoBXAlias, null).add(totoBAlias, null),
							Maps.forHashMap(String.class, Object.class)
									.add(totoIdAlias, 3).add(totoXAlias, 31).add(totoDiscriminator, "TotoB")
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAlias, null)
									.add(totoBIdAlias, 3).add(totoBXAlias, 37).add(totoBAlias, 41),
							Maps.forHashMap(String.class, Object.class)
									.add(totoIdAlias, 4).add(totoXAlias, 43).add(totoDiscriminator, "TotoB")
									.add(totoAIdAlias, null).add(totoAXAlias, null).add(totoAAlias, null)
									.add(totoBIdAlias, 4).add(totoBXAlias, 43).add(totoBAlias, 53)
					))
			);
			
			// We test the collection criteria through the "q" property which is not really a production use case since it's an "embedded" one
			// made of the storage of a set in an SQL Array : production use case we'll be more a *-to-many case. Meanwhile, this test should pass
			// because even in that case it's hard to retrieve the entities with one select
			ExecutableEntityQueryCriteria<AbstractToto, ?> totoExecutableEntityQueryCriteria = testInstance.selectWhere(AbstractToto::getQ, Operators.eq(Arrays.asHashSet(42)));
			Set<AbstractToto> select = totoExecutableEntityQueryCriteria.execute(Accumulators.toSet());
			
			verify(preparedStatement, times(2)).executeQuery();
			verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).containsExactly(
					"select Toto.id as " + idAlias
							+ ", DISCRIMINATOR as DISCRIMINATOR"
							+ " from ("
							+ "select TotoA.id as " + idAlias
							+ ", TotoA.x as " + xAlias
							+ ", TotoA.q as " + qAlias
							+ ", 'TotoA' as " + totoDTYPEAlias + " from TotoA"
							+ " union all"
							+ " select TotoB.id as " + idAlias
							+ ", TotoB.x as " + xAlias
							+ ", TotoB.q as " + qAlias
							+ ", 'TotoB' as " + totoDTYPEAlias + " from TotoB)"
							+ " as Toto"
							+ " left outer join TotoA as TotoA on Toto.id = TotoA.id left outer join TotoB as TotoB on Toto.id = TotoB.id"
							+ " where Toto.q = ?",
					"select Toto.id as " + totoIdAlias
							+ ", Toto.x as " + totoXAlias
							+ ", Toto.q as Toto_q"
							+ ", Toto.DISCRIMINATOR as " + totoDiscriminator
							+ ", TotoA.a as " + totoAAlias
							+ ", TotoA.id as TotoA_id"
							+ ", TotoA.x as " + totoAXAlias
							+ ", TotoA.q as TotoA_q"
							+ ", TotoB.b as " + totoBAlias
							+ ", TotoB.id as TotoB_id"
							+ ", TotoB.x as " + totoBXAlias
							+ ", TotoB.q as TotoB_q"
							+ " from (select TotoA.id as id"
								+ ", TotoA.x as " + xAlias
								+ ", TotoA.q as " + qAlias
								+ ", 'TotoA' as " + totoDTYPEAlias
								+ " from TotoA"
								+ " union all"
								+ " select TotoB.id as id"
								+ ", TotoB.x as " + xAlias
								+ ", TotoB.q as " + qAlias
								+ ", 'TotoB' as " + totoDTYPEAlias
								+ " from TotoB) as Toto"
								+ " left outer join TotoA as TotoA on Toto.id = TotoA.id"
								+ " left outer join TotoB as TotoB on Toto.id = TotoB.id"
							+ " where Toto.id in (?, ?, ?, ?)"
			);
			// Here we don't test the "42" row of the first query because it requires to listen to PreparedStatement.setArray(..) whereas all this
			// test class is bound to PreparedStatement.setInt(..) and Integer type.
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 1).add(2, 2).add(3, 3).add(4, 4);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(select).usingRecursiveFieldByFieldElementComparator()
					.containsExactlyInAnyOrder(
							new TotoA(1, 17, 23),
							new TotoA(2, 29, 31),
							new TotoB(3, 37, 41),
							new TotoB(4, 43, 53));
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
	
	@Nested
	class OneToTablePerClass {
		
		private final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		
		{
			DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
			DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
			DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
			DIALECT.getSqlTypeRegistry().put(Color.class, "int");
		}
		private final PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), HSQLDBDialectBuilder.defaultHSQLDBDialect());
		
		@Test
		void oneSubClass() {
			EntityPersister<Engine, Identifier<Long>> enginePersister = entityBuilder(Engine.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Engine::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Engine::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
							.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
									.addSubClass(subentityBuilder(Car.class)
											.map(Car::getModel)
											.map(Car::getColor))))
					.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Engine.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			Engine dummyEngine = new Engine(42L);
			dummyEngine.setVehicle(dummyCar);
			
			// insert test
			enginePersister.insert(dummyEngine);
			
			ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from Car", String.class)
					.mapKey("model", String.class);
			
			Set<String> allCars = modelQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactly("Renault");
			
			// update test
			dummyCar.setModel("Peugeot");
			enginePersister.persist(dummyEngine);
			
			Set<String> existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).containsExactly("Peugeot");
			
			// select test
			Engine loadedEngine = enginePersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedEngine).usingRecursiveComparison().isEqualTo(dummyEngine);
			
			// delete test
			enginePersister.delete(dummyEngine);
			
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).isEmpty();
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<Engine, Identifier<Long>> enginePersister = entityBuilder(Engine.class, LONG_TYPE)
					.mapKey(Engine::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Engine::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
							// mapped super class defines id
							.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
							.map(Vehicle::getColor)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
									.addSubClass(subentityBuilder(Car.class)
											.map(Car::getId)
											.map(Car::getModel))
									.addSubClass(subentityBuilder(Truck.class)
											.map(Truck::getId))))
					.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Truck", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Engine.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Engine dummyEngine = new Engine(42L);
			dummyEngine.setVehicle(dummyCar);
			
			// insert test
			enginePersister.insert(dummyEngine);
			
			ExecutableBeanPropertyQueryMapper<Integer> vehicleIdQuery = persistenceContext.newQuery("select id from car union select id from truck", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> vehicleIds = vehicleIdQuery.execute(Accumulators.toSet());
			assertThat(vehicleIds).containsExactly(1);
			
			ExecutableBeanPropertyQueryMapper<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> carIds = carIdQuery.execute(Accumulators.toSet());
			assertThat(carIds).containsExactly(1);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckIdQuery = persistenceContext.newQuery("select id from truck", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> truckIds = truckIdQuery.execute(Accumulators.toSet());
			assertThat(truckIds).isEmpty();
			
			// update test
			Truck dummyTruck = new Truck(2L);
			dummyTruck.setColor(new Color(42));
			
			dummyEngine.setVehicle(dummyTruck);
			enginePersister.persist(dummyEngine);
			
			vehicleIds = vehicleIdQuery.execute(Accumulators.toSet());
			assertThat(vehicleIds).containsExactly(2);
			
			carIds = carIdQuery.execute(Accumulators.toSet());
			assertThat(carIds).isEmpty();	// because we asked for orphan removal
			
			truckIds = truckIdQuery.execute(Accumulators.toSet());
			assertThat(truckIds).containsExactly(2);
			
			
			// select test
			Engine loadedVehicle;
			loadedVehicle = enginePersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedVehicle).usingRecursiveComparison().isEqualTo(dummyEngine);
			
			// delete test
			enginePersister.delete(loadedVehicle);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Truck", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
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
		
		public void setQ(Set<Integer> q) {
			this.q = q;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", id == null ? "null" : id.getDelegate()).add("x", x)
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
					+ Maps.asMap("id", id == null ? "null" : id.getDelegate())
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
					+ Maps.asMap("id", id == null ? "null" : id.getDelegate())
					.add("x", x)
					.add("b", b)
					+ "]";
		}
	}
	
}