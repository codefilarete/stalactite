package org.stalactite.persistence.sql.dml;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
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
		List<Field> fields = persistentFieldHarverster.getFields(Toto.class);
		
		totoClassMapping = new HashMap<>(5);
		Table totoTable = new Table(null, "Toto");
		for (Field field : fields) {
			totoClassMapping.put(field, totoTable.new Column(field.getName(), Integer.class));
		}
		Map<String, Column> columns = totoTable.mapColumnsOnName();
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
		
		CRUDStatement testInstance = new CRUDStatement(Maps.fastMap(colA, 1).put(colB, 2).getMap(), "insert into Toto(A, B) values ?, ?");
		PersistentValues values = new PersistentValues();
		values.putUpsert(colA, 123);
		values.putUpsert(colB, 456);
		PreparedStatement statement = testInstance.apply(values, connection);
		statement.execute();
		
		ResultSet resultSet = connection.prepareStatement("select A, B from Toto").executeQuery();
		resultSet.next();
		Assert.assertEquals(resultSet.getInt("a"), 123);
		Assert.assertEquals(resultSet.getInt("b"), 456);
		
		// second insert
		values.putUpsert(colA, 789);
		values.putUpsert(colB, 0);
		testInstance.apply(values, connection);
		statement.execute();
		
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