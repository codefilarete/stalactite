package org.codefilarete.stalactite.mapping;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.MapBasedColumnedRow;
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
		MapBasedColumnedRow row = new MapBasedColumnedRow();
		row.put(columnA, 1);
		row.put(columnB, "hello");
		
		Toto transform = testInstance.transform(row);
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
