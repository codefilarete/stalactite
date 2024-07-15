package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.InMemoryCounterIdentifierGenerator;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.BuildLifeCycleListener;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
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
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.tool.trace.ModifiableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
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
	private Dialect dialect;
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
	
	protected void initMapping() {
		Field fieldId = Reflections.getField(AbstractToto.class, "id");
		Field fieldA = Reflections.getField(AbstractToto.class, "x");
		Field fieldB = Reflections.getField(AbstractToto.class, "y");
		
		Table totoTable = new Table("Toto");
		Column<Table, Object> idColumn = totoTable.addColumn("id", fieldId.getType()).primaryKey();
		Column<Table, Object> xColumn = totoTable.addColumn("x", fieldA.getType());
		Column<Table, Object> yColumn = totoTable.addColumn("y", fieldB.getType());
		Map<String, Column> columnMap = totoTable.mapColumnsOnName();
		
		PropertyAccessor<AbstractToto, Identifier<Integer>> identifierAccessor = Accessors.propertyAccessor(fieldId);
		Map<PropertyAccessor<AbstractToto, Object>, Column<Table, Object>> totoPropertyMapping = Maps.forHashMap(
				(Class<PropertyAccessor<AbstractToto, Object>>) (Class) PropertyAccessor.class, (Class<Column<Table, Object>>) (Class) Column.class)
				.add((PropertyAccessor) identifierAccessor, idColumn)
				.add(Accessors.propertyAccessor(fieldA), xColumn)
				.add(Accessors.propertyAccessor(fieldB), yColumn);
		
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
		Map<Class<? extends AbstractToto>, ConfiguredPersister<? extends AbstractToto, Identifier<Integer>>> subclasses = Maps.asHashMap(TotoA.class, totoAIdentifierConfiguredPersister);
		// We specify discriminator as an Integer because it's the same type as other tested columns and simplify data capture and comparison
		Column<?, Integer> dtype = totoClassMapping.getTargetTable().addColumn("DTYPE", Integer.class);
		SingleTablePolymorphism<AbstractToto, Integer> polymorphismPolicy = new SingleTablePolymorphism<>(dtype.getName(), dtype.getJavaType());
		polymorphismPolicy.addSubClass(new SubEntityMappingConfiguration<TotoA>() {
			@Override
			public Class<TotoA> getEntityType() {
				return TotoA.class;
			}
			
			@Override
			public EmbeddableMappingConfiguration<TotoA> getPropertiesMapping() {
				return null;
			}
			
			@Override
			public <TRGT, TRGTID> List<OneToOneRelation<TotoA, TRGT, TRGTID>> getOneToOnes() {
				return Collections.emptyList();
			}
			
			@Override
			public <TRGT, TRGTID> List<OneToManyRelation<TotoA, TRGT, TRGTID, ? extends Collection<TRGT>>> getOneToManys() {
				return Collections.emptyList();
			}
			
			@Override
			public List<ElementCollectionRelation<TotoA, ?, ? extends Collection>> getElementCollections() {
				return Collections.emptyList();
			}
			
			@Override
			public PolymorphismPolicy<TotoA> getPolymorphismPolicy() {
				return null;
			}
		}, 100);
		
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
			effectiveBatchedRowCount.setRowCounts(Arrays.asList(new long[] { 1, 1, 1 }, new long[] { 1 }, new long[] { 1, 1, 1 }, new long[] { 1 }));
			testInstance.insert(Arrays.asList(
					new TotoA(17, 23, 117, 123),
					new TotoA(29, 31, 129, 131),
					new TotoA(37, 41, 137, 141),
					new TotoA(43, 53, 143, 153)
			));
			
			verify(preparedStatement, times(4)).addBatch();
			verify(preparedStatement, times(2)).executeLargeBatch();
			// since all columns are of same type (Integer) we can capture them all in one statement, else (different types)
			// it's much more difficult
			verify(preparedStatement, times(24)).setInt(indexCaptor.capture(), valueCaptor.capture());
			assertThat(statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("insert into Toto(a, id, x, y, z, DTYPE) values (?, ?, ?, ?, ?, ?)"));
			PairSetList<Integer, Object> expectedPairs = new PairSetList<Integer, Object>()
					.newRow(1, 123).add(2, 1).add(3, 17).add(4, 23).add(5, 117).add(6, 100)
					.newRow(1, 131).add(2, 2).add(3, 29).add(4, 31).add(5, 129).add(6, 100)
					.newRow(1, 141).add(2, 3).add(3, 37).add(4, 41).add(5, 137).add(6, 100)
					.newRow(1, 153).add(2, 4).add(3, 43).add(4, 53).add(5, 143).add(6, 100);
			assertCapturedPairsEqual(expectedPairs);
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
			ExecutableProjectionQuery<AbstractToto> totoRelationalExecutableEntityQuery = testInstance.selectProjectionWhere(select ->  {
				select.clear();
				select.add(count, "count");
			}, AbstractToto::getX, Operators.eq(77));
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
					"select count(id) as count from Toto where Toto.x = ?"));
			PairSetList<Integer, Object> expectedPairs = new PairSetList<Integer, Object>().newRow(1, 77);
			assertCapturedPairsEqual(expectedPairs);
			
			assertThat(countValue).isEqualTo(42);
		}
	}
	
	void assertCapturedPairsEqual(PairSetList<Integer, Object> expectedPairs) {
		// NB: even if Integer can't be inherited, PairIterator is a Iterator<? extends X, ? extends X>
		List<Duo<Integer, Integer>> obtainedPairs = PairSetList.toPairs(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		List<Set<Duo<Integer, Integer>>> obtained = new ArrayList<>();
		int startIndex = 0;
		for (Set<Duo<Integer, Object>> expectedPair : expectedPairs.asList()) {
			obtained.add(new HashSet<>(obtainedPairs.subList(startIndex, startIndex += expectedPair.size())));
		}
		assertThat(obtained).isEqualTo(expectedPairs.asList());
	}
	
	private static abstract class AbstractToto implements Identified<Integer> {
		private Identifier<Integer> id;
		private Integer x, y, z;
		
		public AbstractToto() {
		}
		
		public AbstractToto(int id, Integer x, Integer y, Integer z) {
			this.id = new PersistableIdentifier<>(id);
			this.x = x;
			this.y = y;
			this.z = z;
		}
		
		public AbstractToto(Integer x, Integer y, Integer z) {
			this.x = x;
			this.y = y;
			this.z = z;
		}
		
		@Override
		public Identifier<Integer> getId() {
			return id;
		}
		
		public Integer getX() {
			return x;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("id", id == null ? "null" : id.getSurrogate()).add("x", x).add("y", y).add("z", z)
					+ "]";
		}
	}
	
	static class TotoA extends AbstractToto {
		
		private Integer a;
		
		TotoA(Integer x, Integer y, Integer z, Integer a) {
			super(x, y, z);
			this.a = a;
		}
		
	}
	
	static class TotoB extends AbstractToto {
		
		private String b;
		
		TotoB(int id, Integer x, Integer y, Integer z) {
			super(id, x, y, z);
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