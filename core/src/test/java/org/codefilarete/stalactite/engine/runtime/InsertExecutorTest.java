package org.codefilarete.stalactite.engine.runtime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.runtime.AbstractVersioningStrategy.VersioningStrategySupport;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.PersistentFieldHarverster;
import org.codefilarete.stalactite.mapping.SinglePropertyIdAccessor;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.mapping.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.TransactionAwareConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * @author Guillaume Mary
 */
class InsertExecutorTest<T extends Table<T>> extends AbstractDMLExecutorMockTest {
	
	private final Dialect dialect = new Dialect(new JavaTypeToSqlTypeMapping()
		.with(Integer.class, "int"));
	
	private InsertExecutor<Toto, Integer, T> testInstance;
	
	@BeforeEach
	void setUp() {
		PersistenceConfiguration<Toto, Integer, T> persistenceConfiguration = giveDefaultPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new InsertExecutor<>(persistenceConfiguration.classMappingStrategy,
			new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, noRowCountCheckWriteOperationFactory, 3);
	}
	
	@Test
	void insert_simpleCase() throws Exception {
		testInstance.insert(Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53)));
		
		verify(jdbcMock.preparedStatement, times(4)).addBatch();
		verify(jdbcMock.preparedStatement, times(2)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(12)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("insert into Toto(a, b, c) values (?, ?, ?)");
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.newRow(1, 1).add(2, 17).add(3, 23)
				.newRow(1, 2).add(2, 29).add(3, 31)
				.newRow(1, 3).add(2, 37).add(3, 41)
				.newRow(1, 4).add(2, 43).add(3, 53);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void insert_mandatoryColumn() {
		Column<T, Object> bColumn = testInstance.getMapping().getTargetTable().mapColumnsOnName().get("b");
		bColumn.setNullable(false);
		
		assertThatThrownBy(() -> testInstance.insert(Arrays.asList(new Toto(null, 23))))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("Error while inserting values for Toto{a=1, b=null, c=23} in statement \"insert into Toto(a, b, c) values (?, ?, ?)\"")
				.hasCause(new BindingException("Expected non null value for : Toto.b"));
	}
	
	@Test
	void insert_listenerIsCalled() {
		SQLOperationListener<Column<Table, Object>> listenerMock = mock(SQLOperationListener.class);
		testInstance.setOperationListener((SQLOperationListener) listenerMock);
		
		ArgumentCaptor<Map<Column<Table, Object>, ?>> statementArgCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<SQLStatement<Column<Table, Object>>> sqlArgCaptor = ArgumentCaptor.forClass(SQLStatement.class);
		
		testInstance.insert(Arrays.asList(new Toto(17, 23), new Toto(29, 31)));
		
		
		Table mappedTable = new Table("Toto");
		Column colA = mappedTable.addColumn("a", Integer.class);
		Column colB = mappedTable.addColumn("b", Integer.class);
		Column colC = mappedTable.addColumn("c", Integer.class);
		verify(listenerMock, times(2)).onValuesSet(statementArgCaptor.capture());
		ExtendedMapAssert.assertThatMap((Map<Column, Integer>) (Map) statementArgCaptor.getAllValues().get(0))
				// since Query contains columns copies we can't compare them through equals() (and since Column doesn't implement equals()/hashCode()
				.usingElementPredicate((entry1, entry2) -> entry1.getKey().getAbsoluteName().equals(entry2.getKey().getAbsoluteName())
						&& entry1.getValue().equals(entry2.getValue()))
				.containsExactlyInAnyOrder(
						entry(colA, 1),
						entry(colB, 17),
						entry(colC, 23));
		ExtendedMapAssert.assertThatMap((Map<Column, Integer>) (Map) statementArgCaptor.getAllValues().get(1))
				// since Query contains columns copies we can't compare them through equals() (and since Column doesn't implement equals()/hashCode()
				.usingElementPredicate((entry1, entry2) -> entry1.getKey().getAbsoluteName().equals(entry2.getKey().getAbsoluteName())
						&& entry1.getValue().equals(entry2.getValue()))
				.containsExactlyInAnyOrder(
						entry(colA, 2),
						entry(colB, 29),
						entry(colC, 31));
		verify(listenerMock, times(1)).onExecute(sqlArgCaptor.capture());
		assertThat(sqlArgCaptor.getValue().getSQL()).isEqualTo("insert into Toto(a, b, c) values (?, ?, ?)");
	}
	
	@Test
	void insert_withVersioningStrategy() throws SQLException {
		InsertExecutor<VersionnedToto, Integer, ?> testInstance;
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		
		PreparedStatement preparedStatement = mock(PreparedStatement.class);
		when(preparedStatement.executeLargeBatch()).thenReturn(new long[] { 1 });
		Connection connection = mock(Connection.class);
		when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
		when(connection.prepareStatement(anyString(), anyInt())).thenReturn(preparedStatement);
		
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class);
		when(connectionProviderMock.giveConnection()).thenReturn(connection);
		
		ConnectionProvider connectionProvider = new TransactionAwareConnectionProvider(connectionProviderMock);
		
		T totoTable = (T) new Table("toto");
		Column pk = totoTable.addColumn("id", Integer.class).primaryKey();
		Column versionColumn = totoTable.addColumn("version", Long.class);
		Map<ReversibleAccessor, Column> mapping = Maps.forHashMap((Class<ReversibleAccessor>) null, (Class<Column>) null)
				.add(PropertyAccessor.fromMethodReference(VersionnedToto::getVersion, VersionnedToto::setVersion), versionColumn)
				.add(PropertyAccessor.fromMethodReference(VersionnedToto::getA, VersionnedToto::setA), pk);
		testInstance = new InsertExecutor<>(new ClassMapping<VersionnedToto, Integer, T>(
				VersionnedToto.class,
				totoTable,
				(Map) mapping,
				PropertyAccessor.fromMethodReference(VersionnedToto::getA, VersionnedToto::setA),
				new AlreadyAssignedIdentifierManager<>(Integer.class, c -> {}, c -> false)),
				new ConnectionConfigurationSupport(connectionProvider, 3), dmlGenerator, new WriteOperationFactory(), 3);
		
		PropertyAccessor<VersionnedToto, Long> versioningAttributeAccessor = PropertyAccessor.fromMethodReference(VersionnedToto::getVersion, VersionnedToto::setVersion);
		testInstance.setVersioningStrategy(new VersioningStrategySupport<>(versioningAttributeAccessor, input -> ++input));
		
		VersionnedToto toto = new VersionnedToto(42, 17, 23);
		testInstance.insert(Arrays.asList(toto));
		assertThat(toto.getVersion()).isEqualTo(1);
		
		// a rollback must revert sequence increment
		testInstance.getConnectionProvider().giveConnection().rollback();
		assertThat(toto.getVersion()).isEqualTo(0);
		
		// multiple rollbacks don't imply multiple sequence decrement
		testInstance.getConnectionProvider().giveConnection().rollback();
		assertThat(toto.getVersion()).isEqualTo(0);
	}

	protected PersistenceConfiguration<Toto, Integer, T> giveAutoGeneratedKeysPersistenceConfiguration() {
		PersistenceConfiguration<Toto, Integer, T> toReturn = new PersistenceConfiguration<>();

		T targetTable = (T) new Table("Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<T, Object>> mappedFileds = persistentFieldHarverster.mapFields(Toto.class, targetTable);
		PropertyAccessor<Toto, Integer> primaryKeyAccessor = Accessors.propertyAccessor(persistentFieldHarverster.getField("a"));
		Column primaryKeyColumn = persistentFieldHarverster.getColumn(primaryKeyAccessor);
		primaryKeyColumn.primaryKey();
		primaryKeyColumn.setAutoGenerated(true);
		
		// changing mapping strategy to add JDBCGeneratedKeysIdentifierManager and GeneratedKeysReader
		IdentifierInsertionManager<Toto, Integer> identifierGenerator = new JDBCGeneratedKeysIdentifierManager<>(
			new SinglePropertyIdAccessor<>(primaryKeyAccessor),
			new GeneratedKeysReaderAsInt(primaryKeyColumn.getName()),
			Integer.class);

		toReturn.classMappingStrategy = new ClassMapping<>(
			Toto.class,
			targetTable,
			mappedFileds,
			primaryKeyAccessor,
			identifierGenerator);
		toReturn.targetTable = targetTable;

		return toReturn;
	}
	
	@Nested
	public class InsertExecutorTest_autoGenerateKeys {
		
		@Test
		void insert_generatedPK() throws Exception {
			PersistenceConfiguration<Toto, Integer, T> persistenceConfiguration = giveAutoGeneratedKeysPersistenceConfiguration();
			// additional configuration for generated keys method capture
			when(jdbcMock.connection.prepareStatement(anyString(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(jdbcMock.preparedStatement);
			
			// Implementing a ResultSet that gives results
			ResultSet generatedKeyResultSetMock = mock(ResultSet.class);
			when(jdbcMock.preparedStatement.getGeneratedKeys()).thenReturn(generatedKeyResultSetMock);
			// the ResultSet instance will be called for all batch operations so values returned must reflect that
			when(generatedKeyResultSetMock.next()).thenReturn(true, true, true, false, true, false);
			when(generatedKeyResultSetMock.getInt(eq("a"))).thenReturn(1, 2, 3, 4);
			// getObject is for null value detection, so values are not really important
			when(generatedKeyResultSetMock.getObject(eq("a"))).thenReturn(1, 2, 3, 4);
			
			// we rebind statement argument capture because by default it's bound to the "non-generating keys" preparedStatement(..) signature 
			when(jdbcMock.connection.prepareStatement(jdbcMock.sqlCaptor.capture(), eq(Statement.RETURN_GENERATED_KEYS))).thenReturn(jdbcMock.preparedStatement);
			
			
			DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
			InsertExecutor<Toto, Integer, ?> testInstance = new InsertExecutor<>(persistenceConfiguration.classMappingStrategy,
					new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, noRowCountCheckWriteOperationFactory, 3);
			List<Toto> totoList = Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53));
			testInstance.insert(totoList);
			
			verify(jdbcMock.preparedStatement, times(4)).addBatch();
			verify(jdbcMock.preparedStatement, times(2)).executeLargeBatch();
			verify(jdbcMock.preparedStatement, times(8)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
			assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("insert into Toto(a, b, c) values (default, ?, ?)");
			PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
					.newRow(1, 17).add(2, 23)
					.newRow(1, 29).add(2, 31)
					.newRow(1, 37).add(2, 41)
					.newRow(1, 43).add(2, 53);
			assertCapturedPairsEqual(jdbcMock, expectedPairs);
			
			verify(generatedKeyResultSetMock, times(6)).next();
			verify(generatedKeyResultSetMock, times(4)).getInt(eq("a"));
			
			// Verfy that database generated keys were set into Java instances
			assertThat(Iterables.collectToList(totoList, toto -> toto.a)).isEqualTo(Arrays.asList(1, 2, 3, 4));
		}
	}
	
	protected static class VersionnedToto extends Toto {
		protected long version;
		
		public VersionnedToto(int a, int b, int c) {
			super(a, b, c);
		}
		
		public Integer getA() {
			return a;
		}
		
		public void setA(Integer a) {
			this.a = a;
		}
		
		public long getVersion() {
			return version;
		}
		
		public void setVersion(long version) {
			this.version = version;
		}
		
	}

	public static class GeneratedKeysReaderAsInt extends GeneratedKeysReader<Integer> {
		public GeneratedKeysReaderAsInt(String keyName) {
			super(keyName, DefaultResultSetReaders.INTEGER_PRIMITIVE_READER);
		}
	}
}