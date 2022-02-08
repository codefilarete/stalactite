package org.gama.stalactite.command.builder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.tool.collection.Maps;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operators;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class DeleteCommandBuilderTest {
	
	@Test
	public void testToSQL_singleTable() {
		Table totoTable = new Table("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		
		Delete<Table> delete = new Delete<Table>(totoTable);
		delete.where(columnA, Operators.eq(44)).or(columnA, Operators.eq(columnB));
		
		DeleteCommandBuilder<Table> testInstance = new DeleteCommandBuilder<>(delete);
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto where a = 44 or a = b");
		
		delete = new Delete<Table>(totoTable);
		
		testInstance = new DeleteCommandBuilder<>(delete);
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto");
		
	}
	
	@Test
	public void testToSQL_multiTable() {
		Table totoTable = new Table("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		Table tataTable = new Table("Tata");
		Column<Table, Long> columnX = tataTable.addColumn("x", Long.class);
		Column<Table, String> columnY = tataTable.addColumn("y", String.class);
		
		Delete<Table> delete = new Delete<Table>(totoTable);
		delete.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		
		DeleteCommandBuilder<Table> testInstance = new DeleteCommandBuilder<>(delete);
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y");
		
		delete = new Delete<Table>(totoTable);
		delete.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		
		testInstance = new DeleteCommandBuilder<>(delete);
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y");
	}
	
	@Test
	public void testToStatement() throws SQLException {
		Table totoTable = new Table("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		
		Delete<Table> delete = new Delete<Table>(totoTable);
		delete.where(columnA,  Operators.in(42L, 43L)).or(columnA, Operators.eq(columnB));
		
		DeleteCommandBuilder<Table> testInstance = new DeleteCommandBuilder<>(delete);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
		
		PreparedSQL result = testInstance.toStatement(binderRegistry);
		assertThat(result.getSQL()).isEqualTo("delete from Toto where a in (?, ?) or a = b");
		
		assertThat(result.getValues()).isEqualTo(Maps.asMap(1, 42L).add(2, 43L));
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		
		PreparedStatement mock = mock(PreparedStatement.class);
		result.applyValues(mock);
		verify(mock).setLong(1, 42L);
		verify(mock).setLong(2, 43L);
		
		// ensuring that column type override in registry is taken into account
		binderRegistry.register(columnA, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result = testInstance.toStatement(binderRegistry);
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result.applyValues(mock);
		verify(mock, times(2)).setLong(1, 42L);
		verify(mock, times(2)).setLong(2, 43L);
	}
	
}