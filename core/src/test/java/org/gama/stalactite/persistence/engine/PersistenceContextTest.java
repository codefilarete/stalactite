package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.matchers.CapturingMatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.stalactite.query.model.Operators.eq;
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
		doNothing().when(preparedStatementMock).setLong(anyInt(), capture(long.class, valuesStatementCaptor));
		doNothing().when(preparedStatementMock).setString(anyInt(), capture(String.class, valuesStatementCaptor));
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new Dialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test insert
		testInstance.insert(totoTable).set(id, 1L).set(name, "Hello world !").execute();
		
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("insert into toto(id, name) values (?, ?)");
		assertThat(valuesStatementCaptor.getAllValues()).containsExactly(1L, "Hello world !");
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
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new Dialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test update
		testInstance.update(totoTable).set(id, 1L).execute();
		
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("update toto set id = ?");
		assertThat(valuesStatementCaptor.getAllValues()).containsExactly(1L);
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
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new Dialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test update with where
		testInstance.update(totoTable).set(id, 42L).where(id, eq(666L)).execute();
		
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
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new Dialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		// test delete
		testInstance.delete(totoTable).where(id, eq(42L)).and(name, eq("Hello world !")).execute();
		
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("delete from toto where id = ? and name = ?");
		assertThat(valuesStatementCaptor.getAllValues()).containsExactly(42L, "Hello world !");
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
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connectionMock), new Dialect());
		
		// test select
		Integer count = testInstance.newQuery("select count(*) as count from Toto", Integer.class)
			.mapKey("count", Integer.class)
			.singleResult().execute();
		
		assertThat(count).isEqualTo(666);
		assertThat(sqlStatementCaptor.getValue()).isEqualTo("select count(*) as count from Toto");
		assertThat(valuesStatementCaptor.getAllValues()).isEmpty();
	}
}