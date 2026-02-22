package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class SelectTest {
	
	@Test
	void giveColumnAliases() {
		Table totoTable = new Table("toto");
		Column<Table, Integer> abcColumn = totoTable.addColumn("abc", Integer.class);
		Column<Table, Integer> defColumn = totoTable.addColumn("def", Integer.class);
		Column<Table, Integer> ghiColumn = totoTable.addColumn("ghi", Integer.class);
		Column<Table, Integer> jklColumn = totoTable.addColumn("jkl", Integer.class);
		Select testInstance = new Select();
		testInstance.add(abcColumn, "HeT4L");
		testInstance.add(defColumn, "Gef75J", ghiColumn, "ae8MO");
		testInstance.add(Maps.forHashMap((Class<Column<?,?>>) (Class) Column.class, String.class)
				.add(jklColumn, "47ETRg")
		);
		testInstance.add("count(*)", Integer.class);
		
		Map<Selectable<?>, String> expectedAliases = Maps.forHashMap((Class<Selectable<?>>) (Class) Selectable.class, String.class)
				.add(abcColumn, "HeT4L")
				.add(defColumn, "Gef75J")
				.add(ghiColumn, "ae8MO")
				.add(jklColumn, "47ETRg");
		assertThat(testInstance.getAliases()).containsAllEntriesOf(expectedAliases);
				
	}
}
