package org.codefilarete.stalactite.persistence.sql.order;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.persistence.sql.order.UpdateCommandBuilder.UpdateStatement;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class UpdateCommandBuilderTest {
	
	@Test
	void toSQL_singleTable() {
		Table totoTable = new Table("Toto");
		Column<Table, String> columnA = totoTable.addColumn("a", String.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		
		Update update = new Update(totoTable)
				.set(columnA)
				.set(columnB);
		update.where(columnA, Operators.eq(44)).or(columnA, Operators.eq(columnB));
		UpdateCommandBuilder testInstance = new UpdateCommandBuilder(update);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = ?, b = ? where a = 44 or a = b");
		
		update = new Update(totoTable)
				.set(columnA, columnB);
		testInstance = new UpdateCommandBuilder(update);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = b");
		
		
		update = new Update(totoTable)
				.set(columnA, "tata");
		testInstance = new UpdateCommandBuilder(update);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = 'tata'");
	}
	
	@Test
	void toSQL_multiTable() {
		Table totoTable = new Table("Toto");
		Column<Table, String> columnA = totoTable.addColumn("a", String.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		Column<Table, String> columnC = totoTable.addColumn("c", String.class);
		Table tataTable = new Table("Tata");
		Column<Table, Long> columnX = tataTable.addColumn("x", Long.class);
		Column<Table, String> columnY = tataTable.addColumn("y", String.class);
		Column<Table, String> columnZ = tataTable.addColumn("z", String.class);
		
		
		Update update = new Update(totoTable)
				.set(columnA)
				.set(columnB);
		update.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		UpdateCommandBuilder testInstance = new UpdateCommandBuilder(update);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto, Tata set Toto.a = ?, Toto.b = ? where Toto.a = Tata.x or Toto.a = Tata.y");
		
		update = new Update(totoTable)
				.set(columnC, columnZ);
		update.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		testInstance = new UpdateCommandBuilder(update);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto, Tata set Toto.c = Tata.z where Toto.a = Tata.x or Toto.a = Tata.y");
	}
	
	@Test
	void toStatement() throws SQLException {
		Table totoTable = new Table("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, Long> columnB = totoTable.addColumn("b", Long.class);
		Column<Table, String> columnC = totoTable.addColumn("c", String.class);
		Column<Table, String> columnD = totoTable.addColumn("d", String.class);
		
		Update update = new Update(totoTable)
				.set(columnA)
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD);
		update.where(columnA, Operators.in(42L, 43L)).or(columnA, Operators.eq(columnB));
		UpdateCommandBuilder testInstance = new UpdateCommandBuilder(update);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
		
		UpdateStatement result = testInstance.toStatement(binderRegistry);
		assertThat(result.getSQL()).isEqualTo("update Toto set a = ?, b = a, c = ?, d = ? where a in (?, ?) or a = b");
				
		assertThat(result.getValues()).isEqualTo(Maps.asMap(2, (Object) "tata").add(4, 42L).add(5, 43L));
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(result.getParameterBinder(3)).isEqualTo(DefaultParameterBinders.STRING_BINDER);
		assertThat(result.getParameterBinder(4)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		assertThat(result.getParameterBinder(5)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		result.setValue(columnA, 41L);
		result.setValue(columnD, "toto");
		
		PreparedStatement mock = mock(PreparedStatement.class);
		result.applyValues(mock);
		verify(mock).setLong(1, 41L);
		verify(mock).setString(2, "tata");
		verify(mock).setString(3, "toto");
		verify(mock).setLong(4, 42L);
		verify(mock).setLong(5, 43L);
		
		// test with post modification of pre-set column
		result.setValue(columnC, "tutu");
		result.applyValues(mock);
		verify(mock).setString(2, "tutu");
		
		// ensuring that column type override in registry is taken into account
		binderRegistry.register(columnA, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result = testInstance.toStatement(binderRegistry);
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result.setValue(columnA, -42l);
		result.setValue(columnD, "toto");
		result.applyValues(mock);
		verify(mock).setLong(1, -42l);
	}
}