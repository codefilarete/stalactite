package org.stalactite.persistence.sql.dml;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CRUDOperationTest {
	
	private Connection connection;
	
	private Map<Field, Column> totoClassMapping;
	private Column colA;
	private Column colB;
	private Column colC;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> sqlCaptor;
	private InsertOperation testInstance;

	@BeforeMethod
	public void setUp() throws SQLException {
		// Nécessaire aux CRUDOperations et RowIterator qui ont besoin du ParamaterBinderRegistry
		PersistenceContext.setCurrent(new PersistenceContext(null, new Dialect(new JavaTypeToSqlTypeMapping())));
		
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Table totoClassTable = new Table(null, "Toto");
		totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		colA = columns.get("a");
		colA.setPrimaryKey(true);
		colB = columns.get("b");
		colC = columns.get("c");
		
		preparedStatement = mock(PreparedStatement.class);
		when(preparedStatement.executeBatch()).thenReturn(new int[] {1});
		connection = mock(Connection.class);
		sqlCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(preparedStatement);
		valueCaptor = ArgumentCaptor.forClass(Integer.class);
		indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		testInstance = new InsertOperation("insert into Toto(A, B) values (?, ?)", Maps.asMap(colA, 1).add(colB, 2));
	}
	
	@Test
	public void testApply_oneInsert() throws Exception {
		PersistentValues values = new PersistentValues();
		values.putUpsertValue(colA, 123);
		values.putUpsertValue(colB, 456);
		testInstance.apply(values, connection);
		testInstance.execute();
		
		verify(connection, times(1)).prepareStatement(anyString());
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(A, B) values (?, ?)", sqlCaptor.getValue());
		assertEquals(Arrays.asList(1, 2), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(123, 456), valueCaptor.getAllValues());
	
		// un second insert ne doit pas redemander un second PreparedStatement
		Mockito.reset(connection, preparedStatement);	// clear counters of sensors
		testInstance.apply(values, connection);
		// prepared statement already created, should'nt happen again
		verify(connection, times(0)).prepareStatement(anyString());
	}
	
	@Test
	public void testApply_multiInsert() throws Exception {
		PersistentValues values1 = new PersistentValues();
		values1.putUpsertValue(colA, 1);
		values1.putUpsertValue(colB, 2);
		PersistentValues values2 = new PersistentValues();
		values2.putUpsertValue(colA, 3);
		values2.putUpsertValue(colB, 4);
		PersistentValues values3 = new PersistentValues();
		values3.putUpsertValue(colA, 5);
		values3.putUpsertValue(colB, 6);
		testInstance.apply(Arrays.asList(values1, values2, values3), connection);
		testInstance.execute();
		
		verify(connection, times(1)).prepareStatement(anyString());
		verify(preparedStatement, times(3)).addBatch();
		verify(preparedStatement, times(6)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(A, B) values (?, ?)", sqlCaptor.getValue());
		assertEquals(Arrays.asList(1, 2, 1, 2, 1, 2), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), valueCaptor.getAllValues());
	}
	
	private static class Toto {
		private Integer a, b, c;
	}
}