package org.gama.stalactite.query.model;

import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class SelectTest {
	
	@Test
	void giveColumnAliases() {
		Table totoTable = new Table("toto");
		Column abcColumn = totoTable.addColumn("abc", Integer.class);
		Column defColumn = totoTable.addColumn("def", Integer.class);
		Column ghiColumn = totoTable.addColumn("ghi", Integer.class);
		Column jklColumn = totoTable.addColumn("jkl", Integer.class);
		Select testInstance = new Select();
		testInstance.add(abcColumn, "HeT4L");
		testInstance.add(defColumn, "Gef75J", ghiColumn, "ae8MO");
		testInstance.add(Maps.forHashMap(Column.class, String.class)
				.add(jklColumn, "47ETRg")
		);
		testInstance.add("count(*)");
		
		assertThat(testInstance.giveColumnAliases()).isEqualTo(Maps.forHashMap(Column.class, String.class)
				.add(abcColumn, "HeT4L")
				.add(defColumn, "Gef75J")
				.add(ghiColumn, "ae8MO")
				.add(jklColumn, "47ETRg"));
				
	}
}