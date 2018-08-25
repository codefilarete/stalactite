package org.gama.stalactite.persistence.mapping;

import org.gama.lang.collection.Maps;
import org.gama.reflection.IMutator;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.gama.reflection.IMutator.mutator;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class ToBeanRowTransformerTest {
	
	@Test
	public void testTransform_defaultCase() {
		Table table = new Table("totoTable");
		Column columnA = table.addColumn("a", int.class);
		Column columnB = table.addColumn("b", String.class);
		ToBeanRowTransformer<Toto> testInstance = new ToBeanRowTransformer<>(Toto.class, Maps
				.asMap(columnA, (IMutator) mutator(Toto::setProp1))
				.add(columnB, mutator(Toto::setProp2)));
		Row row = new Row();
		row.add(columnA.getName(), 1);
		row.add(columnB.getName(), "hello");
		
		Toto transform = testInstance.transform(row);
		assertEquals(1, transform.prop1);
		assertEquals("hello", transform.prop2);
		
	}
	
	@Test
	public void testTransform_withSliding() {
		Table table = new Table("totoTable");
		Column columnA = table.addColumn("a", int.class);
		Column columnB = table.addColumn("b", String.class);
		ToBeanRowTransformer<Toto> testInstance = new ToBeanRowTransformer<>(Toto.class, Maps
				.asMap(columnA, (IMutator) mutator(Toto::setProp1))
				.add(columnB, mutator(Toto::setProp2)));
		testInstance = testInstance.withAliases(column -> {
			if (column == columnA) {
				return "a_slided";
			}
			if (column == columnB) {
				return "b_slided";
			}
			return null;
		});
		Row row = new Row();
		row.add("a_slided", 1);
		row.add("b_slided", "hello");
		
		Toto transform = testInstance.transform(row);
		assertEquals(1, transform.prop1);
		assertEquals("hello", transform.prop2);
		
		testInstance = testInstance.withAliases(column -> {
			if (column == columnA) {
				return "a_slided2";
			}
			if (column == columnB) {
				return "b_slided2";
			}
			return null;
		});
		row = new Row();
		row.add("a_slided2", 1);
		row.add("b_slided2", "hello");
		
		transform = testInstance.transform(row);
		assertEquals(1, transform.prop1);
		assertEquals("hello", transform.prop2);
		
	}
	
	@Test
	public void testGetValueTransform_defaultCase() {
		Table table = new Table("totoTable");
		Column columnA = table.addColumn("a", int.class);
		Column columnB = table.addColumn("b", String.class);
		ToBeanRowTransformer<Toto> testInstance = new ToBeanRowTransformer<>(Toto.class, Maps
				.asMap(columnA, (IMutator) mutator(Toto::setProp1))
				.add(columnB, mutator(Toto::setProp2)));
		Row row = new Row();
		row.add(columnA.getName(), 1);
		row.add(columnB.getName(), "hello");
		
		assertEquals(1, testInstance.getValue(row, columnA));
		assertEquals("hello", testInstance.getValue(row, columnB));
	}
	
	@Test
	public void testGetValueTransform_withSliding() {
		Table table = new Table("totoTable");
		Column columnA = table.addColumn("a", int.class);
		Column columnB = table.addColumn("b", String.class);
		ToBeanRowTransformer<Toto> testInstance = new ToBeanRowTransformer<>(Toto.class, Maps
				.asMap(columnA, (IMutator) mutator(Toto::setProp1))
				.add(columnB, mutator(Toto::setProp2)));
		testInstance = testInstance.withAliases(column -> {
			if (column == columnA) {
				return "a_slided";
			}
			if (column == columnB) {
				return "b_slided";
			}
			return null;
		});
		Row row = new Row();
		row.add("a_slided", 1);
		row.add("b_slided", "hello");
		
		assertEquals(1, testInstance.getValue(row, columnA));
		assertEquals("hello", testInstance.getValue(row, columnB));
	}
	
	private static class Toto {
		
		private int prop1;
		private String prop2;
		
		public void setProp1(int prop1) {
			this.prop1 = prop1;
		}
		
		public void setProp2(String prop2) {
			this.prop2 = prop2;
		}
	}
}