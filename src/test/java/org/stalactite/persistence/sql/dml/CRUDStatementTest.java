package org.stalactite.persistence.sql.dml;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.mapping.FieldMappingStrategy;
import org.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import junit.framework.Assert;

public class CRUDStatementTest {
	
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
		JavaTypeToSqlTypeMapping typeMapping = new JavaTypeToSqlTypeMapping();
		typeMapping.put(Integer.class, "BIGINT");
		DDLGenerator ddlGenerator = new DDLGenerator(typeMapping);
		
		FieldMappingStrategy<Toto> mappingStrategy = new FieldMappingStrategy<>(totoClassMapping);
		String createTotoTable = ddlGenerator.generateCreateTable(mappingStrategy.getTargetTable());
		
		connection.prepareStatement(createTotoTable).execute();
		
		CRUDStatement testInstance = new CRUDStatement(Maps.asMap(colA, 1).add(colB, 2), "insert into Toto(A, B) values (?, ?)");
		PersistentValues values = new PersistentValues();
		values.putUpsertValue(colA, 123);
		values.putUpsertValue(colB, 456);
		testInstance.apply(values, connection);
		testInstance.executeWrite();
		
		ResultSet resultSet = connection.prepareStatement("select A, B from Toto").executeQuery();
		resultSet.next();
		Assert.assertEquals(resultSet.getInt("a"), 123);
		Assert.assertEquals(resultSet.getInt("b"), 456);
		
		// second insert
		values.putUpsertValue(colA, 789);
		values.putUpsertValue(colB, 0);
		testInstance.apply(values, connection);
		testInstance.executeWrite();
		
		resultSet = connection.prepareStatement("select A, B from Toto").executeQuery();
		resultSet.next();
		Assert.assertEquals(resultSet.getInt("a"), 123);
		Assert.assertEquals(resultSet.getInt("b"), 456);
		resultSet.next();
		Assert.assertEquals(resultSet.getInt("a"), 789);
		Assert.assertEquals(resultSet.getInt("b"), 0);
	}
	
	private static class Toto {
		private Integer a, b, c;
	}
}