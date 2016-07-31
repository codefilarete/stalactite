package org.gama.stalactite.persistence.mapping;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Maps;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class FieldMappingStrategyTest {
	
	private static Table targetTable;
	private static Column colA;
	private static Column colB;
	private static Column colC;
	private static Map<Field, Column> classMapping;
	
	@BeforeClass
	public static void setUpClass() {
		targetTable = new Table("Toto");
		colA = targetTable.new Column("a", Integer.class);
		colB = targetTable.new Column("b", Integer.class);
		colC = targetTable.new Column("c", Integer.class);
		classMapping = Maps.asMap(Reflections.findField(Toto.class, "a"), colA)
				.add(Reflections.findField(Toto.class, "b"), colB)
				.add(Reflections.findField(Toto.class, "c"), colC);
	}
	
	private FieldMappingStrategy<Toto> testInstance;
	
	@Before
	public void setUp() {
		testInstance = new FieldMappingStrategy<>(Toto.class, targetTable, classMapping);
	}
	
	@DataProvider
	public static Object[][] testGetInsertValuesData() {
		return new Object[][] {
				{ new Toto(1, 2, 3), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), Maps.asMap(colA, null).add(colB, null).add(colC, null) },
				{ new Toto(null, 2, 3), Maps.asMap(colA, null).add(colB, 2).add(colC, 3) },
		};
	}
	
	@Test
	@UseDataProvider("testGetInsertValuesData")
	public void testGetInsertValues(Toto modified, Map<Column, Object> expectedResult) {
		Map<Column, Object> valuesToInsert = testInstance.getInsertValues(modified);
		
		assertEquals(expectedResult, valuesToInsert);
	}
	
	@DataProvider
	public static Object[][] testGetUpdateValues_diffOnlyData() {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 5, 6), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, null, null), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 23, 42), Maps.asMap(colB, 2).add(colC, 3) },
				{ new Toto(1, null, null), new Toto(1, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(null, null, null), new Toto(null, 2, 3), Maps.asMap(colB, null).add(colC, null) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
		};
	}
	
	@Test
	@UseDataProvider("testGetUpdateValues_diffOnlyData")
	public void testGetUpdateValues_diffOnly(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<UpwhereColumn, Object> valuesToInsert = testInstance.getUpdateValues(modified, unmodified, false);
		
		assertEquals(expectedResult, UpwhereColumn.getUpdateColumns(valuesToInsert));
		assertEquals(new HashMap<Column, Object>(), UpwhereColumn.getWhereColumns(valuesToInsert));
	}
	
	@DataProvider
	public static Object[][] testGetUpdateValues_allColumnsData() {
		return new Object[][] {
				{ new Toto(1, 2, 3), new Toto(1, 2, 42), Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(null, null, null), new Toto(null, null, null), new HashMap<>() },
				{ new Toto(1, 2, 3), null, Maps.asMap(colA, 1).add(colB, 2).add(colC, 3) },
				{ new Toto(1, 2, 3), new Toto(1, 2, 3), new HashMap<>() },
		};
	}
	
	@Test
	@UseDataProvider("testGetUpdateValues_allColumnsData")
	public void testGetUpdateValues_allColumns(Toto modified, Toto unmodified, Map<Column, Object> expectedResult) {
		Map<UpwhereColumn, Object> valuesToInsert = testInstance.getUpdateValues(modified, unmodified, true);
		
		assertEquals(expectedResult, UpwhereColumn.getUpdateColumns(valuesToInsert));
		assertEquals(new HashMap<Column, Object>(), UpwhereColumn.getWhereColumns(valuesToInsert));
	}
	
	@Test
	public void testTransform() {
		Row row = new Row();
		row.put("a", 1);
		row.put("b", 2);
		row.put("c", 3);
		Toto toto = testInstance.transform(row);
		assertEquals(1, (int) toto.a);
		assertEquals(2, (int) toto.b);
		assertEquals(3, (int) toto.c);
	}
	
	private static class Toto {
		private Integer a, b, c;
		
		public Toto() {
			
		}
		
		public Toto(Integer a, Integer b, Integer c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "[" + Maps.asMap("a", a).add("b", b).add("c", c) + "]";
		}
	}

}