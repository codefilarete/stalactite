package org.codefilarete.stalactite.mapping;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
				.forHashMap(Column.class, (Class<Mutator<Toto, Object>>) (Class) Mutator.class)
				.add(columnA, (Mutator) Accessors.mutatorByMethodReference(Toto::setProp1))
				.add(columnB, Accessors.mutatorByMethodReference(Toto::setProp2)));
		Row row = new Row();
		row.add(columnA.getName(), 1);
		row.add(columnB.getName(), "hello");
		
		Toto transform = testInstance.transform(row);
		assertThat(transform.prop1).isEqualTo(1);
		assertThat(transform.prop2).isEqualTo("hello");
		
	}
	
	@Test
	public void testTransform_withAlias() {
		Table table = new Table("totoTable");
		Column columnA = table.addColumn("a", int.class);
		Column columnB = table.addColumn("b", String.class);
		ToBeanRowTransformer<Toto> testInstance = new ToBeanRowTransformer<>(Toto.class, Maps
				.forHashMap(Column.class, (Class<Mutator<Toto, Object>>) (Class) Mutator.class)
				.add(columnA, (Mutator) Accessors.mutatorByMethodReference(Toto::setProp1))
				.add(columnB, Accessors.mutatorByMethodReference(Toto::setProp2)));
		testInstance = testInstance.copyWithAliases(new ColumnedRow(column -> {
			if (column == columnA) {
				return "a_slided";
			}
			if (column == columnB) {
				return "b_slided";
			}
			return null;
		}));
		Row row = new Row();
		row.add("a_slided", 1);
		row.add("b_slided", "hello");
		
		Toto transform = testInstance.transform(row);
		assertThat(transform.prop1).isEqualTo(1);
		assertThat(transform.prop2).isEqualTo("hello");
		
		testInstance = testInstance.copyWithAliases(new ColumnedRow(column -> {
			if (column == columnA) {
				return "a_slided2";
			}
			if (column == columnB) {
				return "b_slided2";
			}
			return null;
		}));
		row = new Row();
		row.add("a_slided2", 1);
		row.add("b_slided2", "hello");
		
		transform = testInstance.transform(row);
		assertThat(transform.prop1).isEqualTo(1);
		assertThat(transform.prop2).isEqualTo("hello");
		
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