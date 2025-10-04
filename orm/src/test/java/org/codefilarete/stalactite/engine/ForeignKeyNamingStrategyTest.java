package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ForeignKeyNamingStrategyTest {

	@Test
	void hash_2FK_targetColumnsHaveSameName_hashMustBeDifferent() {
		Table leftTable = new Table("leftTable");
		Column leftAColumn = leftTable.addColumn("a", String.class);
		Column leftBColumn = leftTable.addColumn("b", String.class);
		Table rightTable = new Table("rightTable");
		Column rightAColumn = rightTable.addColumn("a", String.class);
		Column rightBColumn = rightTable.addColumn("b", String.class);
		Table associationTable = new Table("associationTable");
		Column AColumn = associationTable.addColumn("left_a", String.class);
		Column BColumn = associationTable.addColumn("left_b", String.class);
		Column XColumn = associationTable.addColumn("right_a", String.class);
		Column YColumn = associationTable.addColumn("right_b", String.class);

		String fk1 = ForeignKeyNamingStrategy.HASH.giveName(
				Key.from(associationTable)
						.addColumn(AColumn)
						.addColumn(BColumn)
						.build(),
				Key.from(leftTable)
						.addColumn(leftAColumn)
						.addColumn(leftBColumn)
						.build());

		String fk2 = ForeignKeyNamingStrategy.HASH.giveName(
				Key.from(associationTable)
						.addColumn(XColumn)
						.addColumn(YColumn)
						.build(),
				Key.from(leftTable)
						.addColumn(rightAColumn)
						.addColumn(rightBColumn)
						.build());
		assertThat(fk1).isNotEqualTo(fk2);
	}
}
