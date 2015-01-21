package org.stalactite.persistence.sql.dml;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class CRUDOperationTest {
	
	private Connection connection;
	
	private Map<Field, Column> totoClassMapping;
	private Column colA;
	private Column colB;
	private Column colC;
	
	@BeforeTest
	public void setUp() throws SQLException {
		this.connection = DriverManager.getConnection("jdbc:hsqldb:mem:bdt", "sa", "");
		
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Table totoClassTable = new Table(null, "Toto");
		totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		colA = columns.get("a");
		colA.setPrimaryKey(true);
		colB = columns.get("b");
		colC = columns.get("c");
	}
	
	@Test
	public void testApply() throws Exception {
//		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
//		typeMapping.put(Integer.class, "BIGINT");
//		DDLGenerator ddlGenerator = new DDLGenerator(typeMapping);
//		
//		FieldMappingStrategy<Toto> mappingStrategy = new FieldMappingStrategy<>(totoClassMapping);
//		String createTotoTable = ddlGenerator.generateCreateTable(mappingStrategy.getTargetTable());
//		
//		connection.prepareStatement(createTotoTable).execute();
		
		PreparedStatement preparedStatement = mock(PreparedStatement.class);
		
		Connection connection = mock(Connection.class);
		ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(argument.capture())).thenReturn(preparedStatement);
		
		ArgumentCaptor<Integer> valueCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
//		DataSource dataSource = mock(DataSource.class);
//		when(dataSource.getConnection()).thenReturn(connection);
		
		
		InsertOperation testInstance = new InsertOperation("insert into Toto(A, B) values (?, ?)", Maps.asMap(colA, 1).add(colB, 2));
		PersistentValues values = new PersistentValues();
		values.putUpsertValue(colA, 123);
		values.putUpsertValue(colB, 456);
		testInstance.apply(values, connection);
		testInstance.execute();
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(A, B) values (?, ?)", argument.getValue());
		assertEquals(Arrays.asList(1, 2), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(123, 456), valueCaptor.getAllValues());
		
//		ResultSet resultSet = connection.prepareStatement("select A, B from Toto").executeQuery();
//		resultSet.next();
//		Assert.assertEquals(resultSet.getInt("a"), 123);
//		Assert.assertEquals(resultSet.getInt("b"), 456);
		
		// second insert
		Mockito.reset(connection, preparedStatement);
		indexCaptor.getAllValues().clear();
		valueCaptor.getAllValues().clear();
		
		values.putUpsertValue(colA, 789);
		values.putUpsertValue(colB, 0);
		testInstance.apply(values, connection);
		testInstance.execute();
		
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(2)).setInt(indexCaptor.capture(), valueCaptor.capture());
		// prepared statement already created, should'nt happen again
		verify(connection, times(0)).prepareStatement(anyString());
		assertEquals(Arrays.asList(1, 2), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(789, 0), valueCaptor.getAllValues());
		
		
//		resultSet = connection.prepareStatement("select A, B from Toto").executeQuery();
//		resultSet.next();
//		Assert.assertEquals(resultSet.getInt("a"), 123);
//		Assert.assertEquals(resultSet.getInt("b"), 456);
//		resultSet.next();
//		Assert.assertEquals(resultSet.getInt("a"), 789);
//		Assert.assertEquals(resultSet.getInt("b"), 0);
	}
	
	private static class Toto {
		private Integer a, b, c;
	}
}