package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PreparedSQLAppenderTest {
	
	@Test
	void newSubPart() {
		Table<?> totoTable = new Table<>("Toto");
		PreparedSQLAppender testInstance = new PreparedSQLAppender(new StringSQLAppender(new DMLNameProvider(k -> "Tutu")), new ColumnBinderRegistry());
		testInstance.cat("select * from ")
				// we check value print at normal level
				.catValue("me").cat(", ")
				.newSubPart(new DMLNameProvider(k -> "Tata"))
				.cat( "(select * from ")
				.catTable(totoTable)
				// we check value print in subpart
				.cat(" where a like(").catValue("you").cat("))")
				.close()
				.cat(", ")
				.newSubPart(new DMLNameProvider(k -> "Titi"))
				.cat( "(select * from ")
				.catTable(totoTable)
				.cat(")")
				.close()
				.cat(", ")
				.catTable(totoTable)
				// we check value print after getting out of subpart
				.cat(" where a like(")
				.catValue("me").cat(")");
		
		// Remember: DMLNameProvider is only used for alias finding there by SQL builder, SQL appenders don't use it, so "Toto" is always printed
		// even if we gave aliases to the test instance
		assertThat(testInstance.getSQL()).isEqualTo("select * from ?, (select * from Toto where a like(?)), (select * from Toto), Toto where a like(?)");
		assertThat(testInstance.getValues()).containsExactlyEntriesOf(Maps.forHashMap(Integer.class, Object.class)
				.add(1, "me")
				.add(2, "you")
				.add(3, "me")
		);
	}
}