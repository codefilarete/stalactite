package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.model.UnvaluedVariable;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.SQLParameterParser.Parameter;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ExpandableSQLAppenderTest {
	
	@Test
	void catValue() {
		Table<?> totoTable = new Table<>("Toto");
		Column<?, String> nameCol = totoTable.addColumn("name", String.class);
		ExpandableSQLAppender testInstance = new ExpandableSQLAppender(new ColumnBinderRegistry(), new DMLNameProvider(totoTable.getAliases()::get));
		testInstance.cat("select * from Toto where id = ")
				.catValue(new ValuedVariable<>(42))
				.cat(" and name = ")
				.catValue(nameCol, new UnvaluedVariable<>("name", String.class));
		
		assertThat(testInstance.getSQL()).isEqualTo("select * from Toto where id = :1 and name = :name");
		assertThat(testInstance.getParsedSQL().getSqlSnippets())
				.extracting(sqlSnippet -> sqlSnippet instanceof StringAppender ? sqlSnippet.toString() : sqlSnippet)
				.containsExactly(
				"select * from Toto where id = ",
				new Parameter("1"),
				" and name = ",
				new Parameter("name"),
				"");
		assertThat(testInstance.getValues()).containsAllEntriesOf(Maps.forHashMap(String.class, Object.class)
				.add("1", 42));
	}
	
	@Test
	void toPreparedSQL() {
		Table<?> totoTable = new Table<>("Toto");
		Column<?, String> nameCol = totoTable.addColumn("name", String.class);
		ExpandableSQLAppender testInstance = new ExpandableSQLAppender(new ColumnBinderRegistry(), new DMLNameProvider(totoTable.getAliases()::get));
		testInstance.cat("select * from Toto where id = ")
				.catValue(new ValuedVariable<>(42))
				.cat(" and name = ")
				.catValue(nameCol, new UnvaluedVariable<>("name", String.class));
		
		PreparedSQL preparedSQL;
		// non exhaustive value set : values present in SQL are kept
		preparedSQL = testInstance.toPreparedSQL(Maps.forHashMap(String.class, Object.class).add("name", "John Doe"));
		assertThat(preparedSQL.getSQL()).isEqualTo("select * from Toto where id = ? and name = ?");
		assertThat(preparedSQL.getValues()).isEqualTo(Maps.forHashMap(Integer.class, Object.class).add(1, 42).add(2, "John Doe"));
		
		// setting all values : values present in SQL are replaced
		preparedSQL = testInstance.toPreparedSQL(Maps.forHashMap(String.class, Object.class).add("1", 77).add("name", "John Doe"));
		assertThat(preparedSQL.getSQL()).isEqualTo("select * from Toto where id = ? and name = ?");
		assertThat(preparedSQL.getValues()).isEqualTo(Maps.forHashMap(Integer.class, Object.class).add(1, 77).add(2, "John Doe"));
		
		// "in" values
		preparedSQL = testInstance.toPreparedSQL(Maps.forHashMap(String.class, Object.class).add("1", Arrays.asList(42, 77)).add("name", "John Doe"));
		assertThat(preparedSQL.getSQL()).isEqualTo("select * from Toto where id = ?, ? and name = ?");
		assertThat(preparedSQL.getValues()).isEqualTo(Maps.forHashMap(Integer.class, Object.class).add(1, 42).add(2, 77).add(3, "John Doe"));
		
		// with Variable as values
		preparedSQL = testInstance.toPreparedSQL(Maps.forHashMap(String.class, Object.class).add("1", new ValuedVariable<>(Arrays.asList(42, 77))).add("name", new ValuedVariable<>("John Doe")));
		assertThat(preparedSQL.getSQL()).isEqualTo("select * from Toto where id = ?, ? and name = ?");
		assertThat(preparedSQL.getValues()).isEqualTo(Maps.forHashMap(Integer.class, Object.class).add(1, 42).add(2, 77).add(3, "John Doe"));
		
		// with missing value
		assertThatCode(() -> testInstance.toPreparedSQL(Maps.forHashMap(String.class, Object.class).add("1", new ValuedVariable<>(Arrays.asList(42, 77)))))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Size is not given for parameter name hence expansion is not possible");
	}
	
	@Test
	void toPreparedSQL_severalTimeSameVariable() {
		Table<?> totoTable = new Table<>("Toto");
		Column<?, String> nameCol = totoTable.addColumn("name", String.class);
		ExpandableSQLAppender testInstance = new ExpandableSQLAppender(new ColumnBinderRegistry(), new DMLNameProvider(totoTable.getAliases()::get));
		testInstance.cat("select * from Toto where id = ")
				.catValue(new ValuedVariable<>(42))
				.cat(" and name = ")
				.catValue(nameCol, new UnvaluedVariable<>("name", String.class))
				.cat(" or id = ")
				.catValue(new ValuedVariable<>(77))
				.cat(" and name = ")
				.catValue(nameCol, new UnvaluedVariable<>("name", String.class));
		
		PreparedSQL preparedSQL = testInstance.toPreparedSQL(Maps.forHashMap(String.class, Object.class).add("name", "John Doe"));
		assertThat(preparedSQL.getSQL()).isEqualTo("select * from Toto where id = ? and name = ? or id = ? and name = ?");
		assertThat(preparedSQL.getValues()).isEqualTo(Maps.forHashMap(Integer.class, Object.class).add(1, 42).add(2, "John Doe").add(3, 77).add(4, "John Doe"));
	}
}