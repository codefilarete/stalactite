package org.codefilarete.stalactite.sql.result;

import java.util.HashMap;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RowDecoderByColumnTest {

	@Test
	void put() {
		MapBasedColumnedRow testInstance = new MapBasedColumnedRow(new HashMap<>());
		Table<?> dummyTable = new Table<>("Toto");
		Column<?, String> idColumn = dummyTable.addColumn("id", String.class);
		testInstance.put(idColumn, "hello");
		assertThat(testInstance.get(idColumn)).isEqualTo("hello");
	}
}
