package org.stalactite.persistence.sql.dml;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.Test;

public class DMLGeneratorTest {
	
	@Test
	public void testBuildInsert() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		InsertOperation buildedInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals(buildedInsert.getSql(), "insert into Toto(A, B) values (?, ?)");
		
		Field insertIndexesField = buildedInsert.getClass().getDeclaredField("insertIndexes");
		insertIndexesField.setAccessible(true);
		Map<Column, Integer> upsertIndexes = (Map<Column, Integer>) insertIndexesField.get(buildedInsert);
		assertEquals(upsertIndexes, Maps.asMap(colA, 1).add(colB, 2));
	}
	
	@Test
	public void testBuildUpdate() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		UpdateOperation buildedUpdate = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA));
		assertEquals(buildedUpdate.getSql(), "update Toto set A = ?, B = ? where A = ?");

		Field upsertIndexesField = buildedUpdate.getClass().getDeclaredField("updateIndexes");
		upsertIndexesField.setAccessible(true);
		Map<Column, Integer> upsertIndexes = (Map<Column, Integer>) upsertIndexesField.get(buildedUpdate);
		assertEquals(upsertIndexes, Maps.asMap(colA, 1).add(colB, 2));
		
		Field whereIndexesField = buildedUpdate.getClass().getDeclaredField("whereIndexes");
		whereIndexesField.setAccessible(true);
		Map<Column, Integer> whereIndexes = (Map<Column, Integer>) whereIndexesField.get(buildedUpdate);
		assertEquals(whereIndexes, Maps.asMap(colA, 3));
	}
	
	@Test
	public void testBuildDelete() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		DeleteOperation buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals(buildedDelete.getSql(), "delete Toto where A = ?");
		
		Field whereIndexesField = buildedDelete.getClass().getDeclaredField("whereIndexes");
		whereIndexesField.setAccessible(true);
		Map<Column, Integer> whereIndexes = (Map<Column, Integer>) whereIndexesField.get(buildedDelete);
		assertEquals(whereIndexes, Maps.asMap(colA, 1));
	}
	
	@Test
	public void testBuildSelect() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		SelectOperation buildedSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA));
		assertEquals(buildedSelect.getSql(), "select A, B from Toto where A = ?");
		
		Field whereIndexesField = buildedSelect.getClass().getDeclaredField("whereIndexes");
		whereIndexesField.setAccessible(true);
		Map<Column, Integer> whereIndexes = (Map<Column, Integer>) whereIndexesField.get(buildedSelect);
		assertEquals(whereIndexes, Maps.asMap(colA, 1));
	}
}