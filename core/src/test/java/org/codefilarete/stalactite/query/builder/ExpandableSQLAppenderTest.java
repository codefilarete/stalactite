package org.codefilarete.stalactite.query.builder;

import java.util.HashMap;

import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
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
				.catValue(nameCol, new Placeholder<>("name", String.class));
		
		assertThat(testInstance.getSQL()).isEqualTo("select * from Toto where id = :1 and name = :name");
		assertThat(testInstance.getSqlSnippets())
				.extracting(sqlSnippet -> sqlSnippet instanceof StringSQLAppender ? ((StringSQLAppender) sqlSnippet).getSQL() : sqlSnippet)
				.usingRecursiveFieldByFieldElementComparator()
				.containsExactly(
				"select * from Toto where id = ",
				new Placeholder<>("1", int.class),
				" and name = ",
				new Placeholder<>("name", String.class),
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
				.catValue(nameCol, new Placeholder<>("name", String.class));
		
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
		ExpandableSQLAppender testInstance = new ExpandableSQLAppender(new ColumnBinderRegistry(), new DMLNameProvider(new HashMap<>()));
		testInstance.cat("select * from Toto where id = ")
				.catValue(new ValuedVariable<>(42))
				.cat(" and name = ")
				.catValue(nameCol, new Placeholder<>("name", String.class))
				.cat(" or id = ")
				.catValue(new ValuedVariable<>(77))
				.cat(" and name = ")
				.catValue(nameCol, new Placeholder<>("name", String.class));
		
		PreparedSQL preparedSQL = testInstance.toPreparedSQL(Maps.forHashMap(String.class, Object.class).add("name", "John Doe"));
		assertThat(preparedSQL.getSQL()).isEqualTo("select * from Toto where id = ? and name = ? or id = ? and name = ?");
		assertThat(preparedSQL.getValues()).isEqualTo(Maps.forHashMap(Integer.class, Object.class).add(1, 42).add(2, "John Doe").add(3, 77).add(4, "John Doe"));
	}
	
	@Test
	void newSubPart() {
		Table<?> totoTable = new Table<>("Toto");
		ExpandableSQLAppender testInstance = new ExpandableSQLAppender(new ColumnBinderRegistry(), new DMLNameProvider(k -> "Tutu"));
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
		
		assertThat(testInstance.getSQL()).isEqualTo("select * from :1, (select * from Toto where a like(:2)), (select * from Toto), Toto where a like(:3)");
		
		PreparedSQL preparedSQL = testInstance.toPreparedSQL(new HashMap<>());
		// Remember: DMLNameProvider is only used for alias finding there by SQL builder, SQL appenders don't use it, so "Toto" is always printed
		// even if we gave aliases to the test instance
		assertThat(preparedSQL.getSQL()).isEqualTo("select * from ?, (select * from Toto where a like(?)), (select * from Toto), Toto where a like(?)");
		assertThat(preparedSQL.getValues()).isEqualTo(Maps.forHashMap(Integer.class, Object.class)
				.add(1, "me")
				.add(2, "you")
				.add(3, "me"));
	}
}