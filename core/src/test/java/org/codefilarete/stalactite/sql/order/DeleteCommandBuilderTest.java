package org.codefilarete.stalactite.sql.order;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * @author Guillaume Mary
 */
class DeleteCommandBuilderTest {
	
	@Test
	void toSQL_singleTable() {
		Table totoTable = new Table("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		
		Delete delete = new Delete(totoTable);
		delete.where(columnA, Operators.eq(44)).or(columnA, Operators.eq(columnB));
		
		DeleteCommandBuilder testInstance = new DeleteCommandBuilder(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto where a = 44 or a = b");
		
		delete = new Delete(totoTable);
		
		testInstance = new DeleteCommandBuilder(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto");
		
	}
	
	@Test
	void toSQL_multiTable() {
		Table totoTable = new Table("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		Table tataTable = new Table("Tata");
		Column<Table, Long> columnX = tataTable.addColumn("x", Long.class);
		Column<Table, String> columnY = tataTable.addColumn("y", String.class);
		
		Delete delete = new Delete(totoTable);
		delete.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		
		DeleteCommandBuilder testInstance = new DeleteCommandBuilder(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y");
		
		delete = new Delete(totoTable);
		delete.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		
		testInstance = new DeleteCommandBuilder(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y");
	}
	
	@Test
	void toStatement() throws SQLException {
		Table totoTable = new Table("Toto");
		Column<Table, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Table, String> columnB = totoTable.addColumn("b", String.class);
		
		Delete delete = new Delete(totoTable);
		delete.where(columnA,  Operators.in(42L, 43L)).or(columnA, Operators.eq(columnB));
		
		Dialect dialect = new Dialect();
		DeleteCommandBuilder testInstance = new DeleteCommandBuilder(delete, dialect);
		
		PreparedSQL result = testInstance.toStatement();
		assertThat(result.getSQL()).isEqualTo("delete from Toto where a in (?, ?) or a = b");
		
		assertThat(result.getValues()).isEqualTo(Maps.asMap(1, 42L).add(2, 43L));
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.LONG_BINDER);
		
		PreparedStatement mock = mock(PreparedStatement.class);
		result.applyValues(mock);
		verify(mock).setLong(1, 42L);
		verify(mock).setLong(2, 43L);
		
		// ensuring that column type override in registry is taken into account
		dialect.getColumnBinderRegistry().register(columnA, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result = testInstance.toStatement();
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result.applyValues(mock);
		verify(mock, times(2)).setLong(1, 42L);
		verify(mock, times(2)).setLong(2, 43L);
	}
	
}