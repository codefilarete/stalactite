package org.codefilarete.stalactite.sql.order;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class InsertCommandBuilderTest {
	
	@Test
	<T extends Table<T>> void testToSQL() {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		Insert<T> insert = new Insert<>(totoTable)
				.set(columnA)
				.set(columnB, "tata");
		
		InsertCommandBuilder<T> testInstance = new InsertCommandBuilder<>(insert, DMLNameProvider::new);
		
		assertThat(testInstance.toSQL()).isEqualTo("insert into Toto(a, b) values (?, 'tata')");
	}
	
	@Test
	<T extends Table<T>> void testToStatement() throws SQLException {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Insert<T> insert = new Insert<>(totoTable)
				.set(columnA)
				.set(columnB, "tata")
				.set(columnC);
		
		InsertCommandBuilder<T> testInstance = new InsertCommandBuilder<>(insert, DMLNameProvider::new);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
		
		InsertStatement<T> result = testInstance.toStatement(binderRegistry);
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