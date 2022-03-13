package org.codefilarete.stalactite.sql.order;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.order.Insert;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class InsertCommandBuilderTest {
	
	@Test
	public void testToSQL() {
		Table totoTable = new Table<>("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		Insert<Table> insert = new Insert<Table>(totoTable)
				.set(columnA)
				.set(columnB, "tata");
		
		InsertCommandBuilder<Table> testInstance = new InsertCommandBuilder<>(insert);
		
		assertThat(testInstance.toSQL()).isEqualTo("insert into Toto(a, b) values (?, 'tata')");
	}
	
	@Test
	public void testToStatement() throws SQLException {
		Table totoTable = new Table<>("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		Column<Table, String> columnC = totoTable.addColumn("c", String.class);
		Insert<Table> insert = new Insert<Table>(totoTable)
				.set(columnA)
				.set(columnB, "tata")
				.set(columnC);
		
		InsertCommandBuilder<Table> testInstance = new InsertCommandBuilder<>(insert);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
//		binderRegistry.register(columnA, DefaultParameterBinders.INTEGER_BINDER);
//		binderRegistry.register(columnB, DefaultParameterBinders.STRING_BINDER);
//		binderRegistry.register(columnC, DefaultParameterBinders.STRING_BINDER);
		
		InsertStatement<Table> result = testInstance.toStatement(binderRegistry);
		assertThat(result.getSQL()).isEqualTo("insert into Toto(a, b, c) values (?, ?, ?)");
		
		assertThat(result.getValues()).isEqualTo(Maps.asMap(2, (Object) "tata"));
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(result.getParameterBinder(3)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		
		result.setValue(columnA, 42L);
		result.setValue(columnC, "toto");
		
		PreparedStatement mock = mock(PreparedStatement.class);
		result.applyValues(mock);
		verify(mock).setLong(1, 42L);
		verify(mock).setString(2, "tata");
		verify(mock).setString(3, "toto");
		
		// ensuring that column type override in registry is taken into account
		binderRegistry.register(columnA, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result = testInstance.toStatement(binderRegistry);
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result.setValue(columnA, -42l);
		result.setValue(columnC, "toto");
		result.applyValues(mock);
		verify(mock).setLong(1, -42l);

	}
	
}