package org.codefilarete.stalactite.engine.runtime;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.DefaultDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.PairIterator;
import org.codefilarete.tool.exception.NotImplementedException;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.test.PairSetList.pairSetList;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class EntityMappingTreeSelectExecutorTest {
	
	@BeforeEach
	void initEntityCandidates() {
		PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext(mock(PersisterRegistry.class)));
	}
	
	@AfterEach
	void removeEntityCandidates() {
		PersisterBuilderContext.CURRENT.remove();
	}
	
	static ClassMapping buildMappingStrategyMock(Table table) {
		ClassMapping mappingStrategyMock = mock(ClassMapping.class);
		when(mappingStrategyMock.getTargetTable()).thenReturn(table);
		// the selected columns are plugged on the table ones
		when(mappingStrategyMock.getSelectableColumns()).thenAnswer(invocation -> table.getColumns());
		return mappingStrategyMock;
	}
	
	public static Object[][] selectData() {
		Table tableWith1ColumnedPK = new Table("Toto");
		Column id1PK = tableWith1ColumnedPK.addColumn("id1", int.class).primaryKey();
		Table tableWith2ColumnedPK = new Table("Toto");
		Column id2PK1 = tableWith2ColumnedPK.addColumn("id1", int.class).primaryKey();
		Column id2PK2 = tableWith2ColumnedPK.addColumn("id2", int.class).primaryKey();
		Table tableWith3ColumnedPK = new Table("Toto");
		Column id3PK1 = tableWith3ColumnedPK.addColumn("id1", int.class).primaryKey();
		Column id3PK2 = tableWith3ColumnedPK.addColumn("id2", int.class).primaryKey();
		Column id3PK3 = tableWith3ColumnedPK.addColumn("id3", int.class).primaryKey();
		
		// please not that input values will be Arrays.asList(11, 13, 17, 23);
		return new Object[][] {
				{ tableWith1ColumnedPK, Arrays.asList(
						"select Toto.id1 as Toto_id1 from Toto where Toto.id1 in (?, ?, ?)",
						"select Toto.id1 as Toto_id1 from Toto where Toto.id1 in (?)"),
						pairSetList(1, 11).add(2, 13).add(3, 17)
								.newRow(1, 23) },
				{ tableWith2ColumnedPK, Arrays.asList(
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2 from Toto where (Toto.id1, Toto.id2) in ((?, ?), (?, ?), (?, ?))",
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2 from Toto where (Toto.id1, Toto.id2) in ((?, ?))"),
						pairSetList(1, 11).add(2, 11).add(3, 13).add(4, 13).add(5, 17).add(6, 17)
								.newRow(1, 23).add(2, 23)
				},
				{ tableWith3ColumnedPK, Arrays.asList(
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2, Toto.id3 as Toto_id3 from Toto where (Toto.id1, Toto.id2, Toto.id3)" +
						" in ((?, ?, ?), (?, ?, ?), (?, ?, ?))",
						"select Toto.id1 as Toto_id1, Toto.id2 as Toto_id2, Toto.id3 as Toto_id3 from Toto where (Toto.id1, Toto.id2, Toto.id3)" +
						" in ((?, ?, ?))"),
						pairSetList(1, 11).add(2, 11).add(3, 11).add(4, 13).add(5, 13).add(6, 13).add(7, 17).add(8, 17).add(9, 17)
								.newRow(1, 23).add(2, 23).add(3, 23)
				},
		};
	}
	
	@ParameterizedTest
	@MethodSource("selectData")
	public <T extends Table<T>> void select(T targetTable, List<String> expectedSql, PairSetList<Integer, Integer> expectedParameters) throws SQLException {
		ClassMapping<Object, Object, T> classMappingStrategy = buildMappingStrategyMock(targetTable);
		// mocking to prevent NPE from EntityMappingTreeSelectExecutor constructor
		IdMapping idMappingMock = mock(IdMapping.class);
		when(classMappingStrategy.getIdMapping()).thenReturn(idMappingMock);
		IdentifierAssembler t;
		PrimaryKey<T, Object> primaryKey = targetTable.getPrimaryKey();
		if (!primaryKey.isComposed()) {
			t = new SimpleIdentifierAssembler(Iterables.first(primaryKey.getColumns()));
		} else {
			t = new ComposedIdentifierAssembler<Object, T>(primaryKey) {
				@Nullable
				@Override
				public Object assemble(Function<Column<?, ?>, Object> columnValueProvider) {
					// this method is not called so we don't need to implement it
					throw new NotImplementedException("Method is not expected to be called");
				}
				
				@Override
				public Map<Column<T, ?>, Object> getColumnValues(Object id) {
					// for simplicity we give the same value to all columns
					return Iterables.map(primaryKey.getColumns(), Function.identity(), c -> id);
				}
			};
		}
		when(idMappingMock.getIdentifierAssembler()).thenReturn(t);
		
		DefaultDialect dialect = new DefaultDialect();
		// we set a in operator size to test overflow
		dialect.setInOperatorMaxSize(3);
		
		JdbcArgCaptor jdbcArgCaptor = new JdbcArgCaptor();
		ConnectionProvider connectionProvider = new SimpleConnectionProvider(jdbcArgCaptor.connection);
		EntityMappingTreeSelectExecutor testInstance = new EntityMappingTreeSelectExecutor<>(classMappingStrategy, dialect, connectionProvider);
		testInstance.prepareQuery();
		
		List<Integer> inputValues = Arrays.asList(11, 13, 17, 23);
		testInstance.select(inputValues);
		
		// one query because in operator is bounded to 3 values
		int expectedQueryCount = (int) Math.ceil(((double) inputValues.size()) / dialect.getInOperatorMaxSize());
		verify(jdbcArgCaptor.preparedStatement, times(expectedQueryCount)).executeQuery();
		int expectedParamCount = inputValues.size() * primaryKey.getColumns().size();
		verify(jdbcArgCaptor.preparedStatement, times(expectedParamCount)).setInt(jdbcArgCaptor.indexCaptor.capture(), jdbcArgCaptor.valueCaptor.capture());
		
		assertThat(jdbcArgCaptor.statementArgCaptor.getAllValues()).isEqualTo(expectedSql);
		
		// Comparing indexes and values : we need to convert captured values to same format as the expected one
		// We reassociate each couple to its query ("line") depending on its position in the captured values
		PairIterator<Integer, Integer> capturedValues = new PairIterator<>(jdbcArgCaptor.indexCaptor.getAllValues(), jdbcArgCaptor.valueCaptor.getAllValues());
		PairSetList<Object, Object> capturedValuesAsPairSetList = new PairSetList<>();
		int paramCountPerQuery = primaryKey.getColumns().size() * dialect.getInOperatorMaxSize();
		Iterables.iterate(capturedValues, (i, c) -> {
			// Because PairSetList contains already an empty line at instantiation time we should not add one at very first iteration (i != 0)  
			if (i != 0 && i % paramCountPerQuery == 0) {
				capturedValuesAsPairSetList.newRow();
			}
			capturedValuesAsPairSetList.add(c.getLeft(), c.getRight());
		});
		assertThat(capturedValuesAsPairSetList).isEqualTo(expectedParameters);
	}
	
	@Test
	public <T extends Table<T>> void select_argumentWithOneBlock() throws SQLException {
		Table dummyTable = new Table("dummyTable");
		Column dummyPK = dummyTable.addColumn("dummyPK", Integer.class).primaryKey();
		ClassMapping<Object, Object, T> classMappingStrategy = buildMappingStrategyMock(dummyTable);
		// mocking to prevent NPE from EntityMappingTreeSelectExecutor constructor
		IdMapping idMappingMock = mock(IdMapping.class);
		when(classMappingStrategy.getIdMapping()).thenReturn(idMappingMock);
		when(idMappingMock.getIdentifierAssembler()).thenReturn(new SimpleIdentifierAssembler(dummyPK));
		
		DefaultDialect dialect = new DefaultDialect();
		// we set a in operator size to test overflow
		dialect.setInOperatorMaxSize(3);
		
		JdbcArgCaptor jdbcArgCaptor = new JdbcArgCaptor();
		ConnectionProvider connectionProvider = new SimpleConnectionProvider(jdbcArgCaptor.connection);
		
		EntityMappingTreeSelectExecutor testInstance = new EntityMappingTreeSelectExecutor<>(classMappingStrategy, dialect, connectionProvider);
		testInstance.prepareQuery();
		testInstance.select(Arrays.asList(11, 13));
		
		// one query because in operator is bounded to 3 values
		verify(jdbcArgCaptor.preparedStatement, times(1)).executeQuery();
		verify(jdbcArgCaptor.preparedStatement, times(2)).setInt(jdbcArgCaptor.indexCaptor.capture(), jdbcArgCaptor.valueCaptor.capture());
		
		assertThat(jdbcArgCaptor.statementArgCaptor.getAllValues()).isEqualTo(Arrays.asList("select dummyTable.dummyPK as dummyTable_dummyPK from " 
				+ "dummyTable where dummyTable.dummyPK in (?, ?)"));
		
		List<Duo<Integer, Integer>> expectedPairs = Arrays.asList(new Duo<>(1, 11), new Duo<>(2, 13));
		PairIterator<Integer, Integer> capturedValues = new PairIterator<>(jdbcArgCaptor.indexCaptor.getAllValues(), jdbcArgCaptor.valueCaptor.getAllValues());
		assertThat(Iterables.copy(capturedValues)).isEqualTo(expectedPairs);
	}
	
	@Test
	public <T extends Table<T>> void select_emptyArgument() {
		T dummyTable = (T) new Table("dummyTable");
		dummyTable.addColumn("id", long.class).primaryKey();
		ClassMapping<Object, Object, T> classMappingStrategy = buildMappingStrategyMock(dummyTable);
		// mocking to prevent NPE from EntityMappingTreeSelectExecutor constructor
		IdMapping idMappingMock = mock(IdMapping.class);
		when(classMappingStrategy.getIdMapping()).thenReturn(idMappingMock);
		
		List<String> capturedSQL = new ArrayList<>();
		DefaultDialect dialect = new DefaultDialect();
		// we set a in operator size to test overflow
		dialect.setInOperatorMaxSize(3);
		ConnectionProvider connectionProvider = mock(ConnectionProvider.class);
		EntityMappingTreeSelectExecutor<Object, Object, ?> testInstance = new EntityMappingTreeSelectExecutor<Object, Object, T>(
			classMappingStrategy, dialect, connectionProvider) {
			
			@Override
			InternalExecutor newInternalExecutor(EntityTreeQuery<Object> entityTreeQuery) {
				return new InternalExecutor(entityTreeQuery, connectionProvider) {
					@Override
					List<Object> execute(String sql, Collection<? extends List<Object>> idsParcels,
										 Map<Column<T, ?>, int[]> inOperatorValueIndexes) {
						capturedSQL.add(sql);
						return Collections.emptyList();
					}
				};
			}
		};
		testInstance.prepareQuery();
		testInstance.select(Arrays.asList());
		assertThat(capturedSQL.isEmpty()).isTrue();
	}
	
	@Test
	public <T extends Table<T>> void execute_realLife_composedId() throws SQLException {
		DataSource dataSource = new HSQLDBInMemoryDataSource(); 
		T targetTable = (T) new Table("Toto");
		Column<T, Long> id1 = targetTable.addColumn("id1", long.class).primaryKey();
		Column<T, Long> id2 = targetTable.addColumn("id2", long.class).primaryKey();
		Column<T, String> name = targetTable.addColumn("name", String.class);
		
		ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(HSQLDBDialectBuilder.defaultHSQLDBDialect().getDdlTableGenerator(), HSQLDBDialectBuilder.defaultHSQLDBDialect().getDdlSequenceGenerator(), connectionProvider);
		ddlDeployer.getDdlGenerator().addTables(targetTable);
		ddlDeployer.deployDDL();
		
		Toto entity1 = new Toto(100, 1, "entity1");
		Toto entity2 = new Toto(200, 2, "entity2");
		Connection currentConnection = connectionProvider.giveConnection();
		PreparedStatement insertStatement = currentConnection.prepareStatement("insert into Toto(id1, id2, name) values (?, ?, ?)");
		insertStatement.setLong(1, entity1.id1);
		insertStatement.setLong(2, entity1.id2);
		insertStatement.setString(3, entity1.name);
		insertStatement.addBatch();
		insertStatement.setLong(1, entity2.id1);
		insertStatement.setLong(2, entity2.id2);
		insertStatement.setString(3, entity2.name);
		insertStatement.addBatch();
		insertStatement.executeBatch();
		insertStatement.close();
		currentConnection.commit();
		
		IdAccessor<Toto, Toto> idAccessor =
				new AccessorWrapperIdAccessor<>(Accessors.accessorByMethodReference(SerializableFunction.identity(), (toto, toto2) -> {
					toto.id1 = toto2.id1;
					toto.id2 = toto2.id2;
				}));
		ClassMapping<Toto, Toto, T> classMappingStrategy = new ClassMapping<Toto, Toto, T>(Toto.class, targetTable,
				(Map) Maps.asMap((ReversibleAccessor) Accessors.accessorByMethodReference(Toto::getId1, Toto::setId1), (Column) id1)
						.add(Accessors.accessorByMethodReference(Toto::getId2, Toto::setId2), id2)
						.add(Accessors.accessorByMethodReference(Toto::getName, Toto::setName), name),
				new ComposedIdMapping<>(idAccessor, new AlreadyAssignedIdentifierManager<>(Toto.class, c -> {}, c -> false)
						, new ComposedIdentifierAssembler<Toto, T>(targetTable) {
					@Override
					public Toto assemble(Function<Column<?, ?>, Object> columnValueProvider) {
						return new Toto((long) columnValueProvider.apply(id1), (long) columnValueProvider.apply(id2));
					}
					
					@Override
					public Map<Column<T, ?>, Object> getColumnValues(Toto id) {
						return Maps.forHashMap((Class<Column<T, ?>>) null, Object.class).add(id1, id.id1).add(id2, id.id2);
					}
				})
		);
		
		// Checking that selected entities by their id are those expected
		EntityMappingTreeSelectExecutor<Toto, Toto, ?> testInstance = new EntityMappingTreeSelectExecutor<>(classMappingStrategy, HSQLDBDialectBuilder.defaultHSQLDBDialect(), connectionProvider);
		testInstance.prepareQuery();
		Set<Toto> select = testInstance.select(Arrays.asList(new Toto(100, 1)));
		assertThat(select.toString()).isEqualTo(Arrays.asList(entity1).toString());
		
		select = testInstance.select(Arrays.asList(new Toto(100, 1), new Toto(200, 2)));
		Comparator<Toto> totoComparator = Comparator.<Toto, Comparable>comparing(toto -> 31 * toto.getId1() + toto.getId2());
		assertThat(Arrays.asTreeSet(totoComparator, select).toString()).isEqualTo(Arrays.asTreeSet(totoComparator, entity1, entity2).toString());
	}
	
	private static class Toto {
		private long id1;
		private long id2;
		private String name;
		
		public Toto() {
		}
		
		public Toto(long id1, long id2) {
			this.id1 = id1;
			this.id2 = id2;
		}
		
		public Toto(long id1, long id2, String name) {
			this.id1 = id1;
			this.id2 = id2;
			this.name = name;
		}
		
		public long getId1() {
			return id1;
		}
		
		public void setId1(long id1) {
			this.id1 = id1;
		}
		
		public long getId2() {
			return id2;
		}
		
		public void setId2(long id2) {
			this.id2 = id2;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		@Override
		public String toString() {
			return "Toto{id1=" + id1 + ", id2=" + id2 + ", name='" + name + '\'' + '}';
		}
	}
	
	protected static class JdbcArgCaptor {
		
		protected final Dialect dialect = new DefaultDialect(new JavaTypeToSqlTypeMapping()
				.with(Integer.class, "int"));
		
		protected PreparedStatement preparedStatement;
		protected ArgumentCaptor<Integer> valueCaptor;
		protected ArgumentCaptor<Integer> indexCaptor;
		protected ArgumentCaptor<String> statementArgCaptor;
		protected Connection connection;
		
		protected JdbcArgCaptor() throws SQLException {
			
			preparedStatement = mock(PreparedStatement.class);
			when(preparedStatement.executeLargeBatch()).thenReturn(new long[] { 1 });
			
			connection = mock(Connection.class);
			// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
			// weither or not it should prepare statement
			when(preparedStatement.getConnection()).thenReturn(connection);
			statementArgCaptor = ArgumentCaptor.forClass(String.class);
			when(connection.prepareStatement(statementArgCaptor.capture())).thenReturn(preparedStatement);
			when(connection.prepareStatement(statementArgCaptor.capture(), anyInt())).thenReturn(preparedStatement);
			ResultSet resultSetMock = mock(ResultSet.class);
			when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
			
			valueCaptor = ArgumentCaptor.forClass(Integer.class);
			indexCaptor = ArgumentCaptor.forClass(Integer.class);
		}
	}
}