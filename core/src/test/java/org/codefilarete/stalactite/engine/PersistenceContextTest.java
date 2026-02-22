package org.codefilarete.stalactite.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.codefilarete.stalactite.engine.crud.BatchDelete;
import org.codefilarete.stalactite.engine.crud.BatchInsert;
import org.codefilarete.stalactite.engine.crud.BatchUpdate;
import org.codefilarete.stalactite.sql.ConnectionProvider.DataSourceConnectionProvider;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.CapturingMatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.query.Operators.eq;
import static org.codefilarete.stalactite.query.Operators.equalsArgNamed;
import static org.codefilarete.stalactite.query.model.QueryEase.where;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.internal.util.Primitives.defaultValue;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextTest {
	
	private static <T> T capture(Class<T> type, CapturingMatcher<Object> capturingMatcher) {
		Mockito.argThat(capturingMatcher);
		return defaultValue(type);
	}
	
	@Test
	void insert() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		ArgumentCaptor<String> sqlStatementCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(sqlStatementCaptor.capture())).thenReturn(preparedStatementMock);
		CapturingMatcher<Object> valuesStatementCaptor = new CapturingMatcher<>();
		doNothing().when(preparedStatementMock).setLong(capture(int.class, valuesStatementCaptor), capture(long.class, valuesStatementCaptor));
		doNothing().when(preparedStatementMock).setString(capture(int.class, valuesStatementCaptor), capture(String.class, valuesStatementCaptor));
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new DefaultDialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test insert
		testInstance.insert(totoTable)
				.set(id, 1L)
				.set(name, "Hello world !")
				.execute();
		
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("insert into toto(id, name) values (?, ?)");
		List<Duo<Integer, Object>> statementArgsPairs = new ArrayList<>();
		List<Object> capturedStatementArgs = valuesStatementCaptor.getAllValues();
		for (int i = 0, allValuesSize = capturedStatementArgs.size(); i < allValuesSize; i+=2) {
			int index = (int) capturedStatementArgs.get(i);
			Object value = capturedStatementArgs.get(i + 1);
			statementArgsPairs.add(new Duo<>(index, value));
		}
		assertThat(statementArgsPairs).containsExactlyInAnyOrder(new Duo<>(1, 1L), new Duo<>(2, "Hello world !"));
	}
	
	@Test
	void insert_batch() {
		PersistenceContext testInstance = new PersistenceContext(
				new DataSourceConnectionProvider(new HSQLDBInMemoryDataSource()), new DefaultDialect());
		
		Table totoTable = new Table<>("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class).primaryKey();
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		// test insert
		BatchInsert batchInsert = testInstance.batchInsert(totoTable);
		
		long effectiveWrite;
		effectiveWrite = batchInsert
				.set(id, 1L)
				.set(name, "Hello world !")
				.newRow()
				.set(id, 2L)
				.set(name, "Hello everybody !")
				.execute();
		
		assertThat(effectiveWrite).isEqualTo(2);
		
		List<Long> select = testInstance.select(SerializableFunction.identity(), id);
		assertThat(select).containsExactly(1L, 2L);
		
		// checking that we can reuse the batch insert instance
		effectiveWrite = batchInsert
				.newRow()	// this call to newRow() is not necessary but must be taken into account by internal algorithm
				.set(id, 3L)
				.set(name, "Hello world !")
				.newRow()
				.set(id, 4L)
				.set(name, "Hello everybody !")
				.execute();
		
		assertThat(effectiveWrite).isEqualTo(2);
		
		select = testInstance.select(SerializableFunction.identity(), id);
		assertThat(select).containsExactly(1L, 2L, 3L, 4L);
	}
	
	@Test
	void update() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		ArgumentCaptor<String> sqlStatementCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(sqlStatementCaptor.capture())).thenReturn(preparedStatementMock);
		CapturingMatcher<Object> valuesStatementCaptor = new CapturingMatcher<>();
		doNothing().when(preparedStatementMock).setLong(anyInt(), capture(long.class, valuesStatementCaptor));
		doNothing().when(preparedStatementMock).setString(anyInt(), capture(String.class, valuesStatementCaptor));
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new DefaultDialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test update
		testInstance.update(totoTable).set(id, 1L).execute();
		
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("update toto set id = ?");
		assertThat(valuesStatementCaptor.getAllValues()).containsExactly(1L);
	}
	
	@Test
	void update_batch() {
		PersistenceContext testInstance = new PersistenceContext(
				new DataSourceConnectionProvider(new HSQLDBInMemoryDataSource()), new DefaultDialect());
		
		Table totoTable = new Table<>("toto");
		Column<Table, Long> idColumn = totoTable.addColumn("id", long.class).primaryKey();
		Column<Table, String> nameColumn = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		testInstance.batchInsert(totoTable)
				.set(idColumn, 1L)
				.set(nameColumn, "Hello world !")
				.newRow()
				.set(idColumn, 2L)
				.set(nameColumn, "Hello everybody !")
				.execute();
		
		assertThat(testInstance.select(SerializableFunction.identity(), idColumn)).containsExactly(1L, 2L);
		
		// test update
		BatchUpdate batchUpdate = testInstance.batchUpdate(totoTable, Arrays.asSet(nameColumn), where(idColumn, equalsArgNamed("pk", Long.class)));
		
		long effectiveWrite;
		effectiveWrite = batchUpdate
				.set(nameColumn, "Hello world !")
				.set("pk", 2L)
				.execute();
		
		assertThat(testInstance.select(SerializableFunction.identity(), nameColumn)).containsExactly("Hello world !", "Hello world !");
		
		assertThat(effectiveWrite).isEqualTo(1);
		
		effectiveWrite = batchUpdate
				.set(nameColumn, "Hello John !")
				.set("pk", 1L)
				.newRow()
				.set(nameColumn, "Hello Jane !")
				.set("pk", 2L)
				.execute();
		
		assertThat(testInstance.select(SerializableFunction.identity(), nameColumn)).containsExactly("Hello John !", "Hello Jane !");
		assertThat(effectiveWrite).isEqualTo(2);
	}
	
	@Test
	void update_withWhere() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		ArgumentCaptor<String> sqlStatementCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(sqlStatementCaptor.capture())).thenReturn(preparedStatementMock);
		CapturingMatcher<Object> valuesStatementCaptor = new CapturingMatcher<>();
		doNothing().when(preparedStatementMock).setLong(anyInt(), capture(long.class, valuesStatementCaptor));
		doNothing().when(preparedStatementMock).setString(anyInt(), capture(String.class, valuesStatementCaptor));
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new DefaultDialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test update with where
		testInstance.update(totoTable, where(id, eq(666L)))
				.set(id, 42L)
				.execute();
		
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("update toto set id = ? where id = ?");
		assertThat(valuesStatementCaptor.getAllValues()).containsExactly(42L, 666L);
	}
	
	@Test
	void delete() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		ArgumentCaptor<String> sqlStatementCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(sqlStatementCaptor.capture())).thenReturn(preparedStatementMock);
		CapturingMatcher<Object> valuesStatementCaptor = new CapturingMatcher<>();
		doNothing().when(preparedStatementMock).setLong(anyInt(), capture(long.class, valuesStatementCaptor));
		doNothing().when(preparedStatementMock).setString(anyInt(), capture(String.class, valuesStatementCaptor));
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new DefaultDialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test delete
		testInstance.delete(totoTable, where(id, eq(42L)).and(name, eq("Hello world !"))).execute();
		
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("delete from toto where id = ? and name = ?");
		assertThat(valuesStatementCaptor.getAllValues()).containsExactly(42L, "Hello world !");
	}
	
	@Test
	void delete_batch() {
		PersistenceContext testInstance = new PersistenceContext(
				new DataSourceConnectionProvider(new HSQLDBInMemoryDataSource()), new DefaultDialect());
		
		Table totoTable = new Table<>("toto");
		Column<Table, Long> idColumn = totoTable.addColumn("id", long.class).primaryKey();
		Column<Table, String> nameColumn = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		testInstance.batchInsert(totoTable)
				.set(idColumn, 1L)
				.set(nameColumn, "Hello world !")
				.newRow()
				.set(idColumn, 2L)
				.set(nameColumn, "Hello everybody !")
				.newRow()
				.set(idColumn, 3L)
				.set(nameColumn, "Hello everyone !")
				.execute();
		
		// test delete
		BatchDelete deleteStatement = testInstance.batchDelete(totoTable, where(nameColumn, equalsArgNamed("name", String.class)));
		long execute;
		
		execute = deleteStatement
				.set("name", "Hello world !")
				.newRow()
				.set("name", "Hello everybody !")
				.execute();
		
		assertThat(execute).isEqualTo(2);
		assertThat(testInstance.select(SerializableFunction.identity(), idColumn)).containsExactly(3L);
		
		// testing statement reuse
		execute = deleteStatement
				.set("name", "Hello everyone !")
				.execute();
		
		assertThat(execute).isEqualTo(1);
		assertThat(testInstance.select(SerializableFunction.identity(), idColumn)).isEmpty();
	}
	
	@Test
	void newQuery_singleResult() throws SQLException {
		Connection connectionMock = Mockito.mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		ArgumentCaptor<String> sqlStatementCaptor = ArgumentCaptor.forClass(String.class);
		when(connectionMock.prepareStatement(sqlStatementCaptor.capture())).thenReturn(preparedStatementMock);
		CapturingMatcher<Object> valuesStatementCaptor = new CapturingMatcher<>();
		doNothing().when(preparedStatementMock).setLong(anyInt(), capture(long.class, valuesStatementCaptor));
		doNothing().when(preparedStatementMock).setString(anyInt(), capture(String.class, valuesStatementCaptor));
		when(preparedStatementMock.executeQuery()).thenReturn(new InMemoryResultSet(Arrays.asList(
			Maps.forHashMap(String.class, Object.class).add("id", 42).add("name", "tata").add("count", 666)
		)));
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new DefaultDialect());
		
		// test select
		Integer count = testInstance.newQuery("select count(*) as count from Toto", Integer.class)
			.mapKey("count", Integer.class)
			.execute(Accumulators.getFirst());
		
		assertThat(count).isEqualTo(666);
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("select count(*) as count from Toto");
		assertThat(valuesStatementCaptor.getAllValues()).isEmpty();
	}
}
