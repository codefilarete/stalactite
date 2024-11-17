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
	<T extends Table<T>> void toSQL_singleTable() {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		
		Delete<T> delete = new Delete<>(totoTable);
		delete.where(columnA, Operators.eq(44)).or(columnA, Operators.eq(columnB));
		
		DeleteCommandBuilder<T> testInstance = new DeleteCommandBuilder<>(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto where a = 44 or a = b");
		
		delete = new Delete<>(totoTable);
		
		testInstance = new DeleteCommandBuilder<>(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto");
	}
	
	@Test
	<T1 extends Table<T1>, T2 extends Table<T2>> void toSQL_multiTable() {
		T1 totoTable = (T1) new Table("Toto");
		Column<T1, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T1, String> columnB = totoTable.addColumn("b", String.class);
		T2 tataTable = (T2) new Table("Tata");
		Column<T2, Long> columnX = tataTable.addColumn("x", Long.class);
		Column<T2, String> columnY = tataTable.addColumn("y", String.class);
		
		Delete<T1> delete = new Delete<>(totoTable);
		delete.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		
		DeleteCommandBuilder<T1> testInstance = new DeleteCommandBuilder<>(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y");
		
		delete = new Delete<>(totoTable);
		delete.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		
		testInstance = new DeleteCommandBuilder<>(delete, new Dialect());
		
		assertThat(testInstance.toSQL()).isEqualTo("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y");
	}
	
	@Test
	<T extends Table<T>> void toPreparedSQL() throws SQLException {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		
		Delete<T> delete = new Delete<>(totoTable);
		delete.where(columnA,  Operators.in(42L, 43L)).or(columnA, Operators.eq(columnB));
		
		Dialect dialect = new Dialect();
		DeleteCommandBuilder<T> testInstance = new DeleteCommandBuilder<>(delete, dialect);
		
		PreparedSQL result = testInstance.toPreparedSQL();
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
		result = testInstance.toPreparedSQL();
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		assertThat(result.getParameterBinder(2)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result.applyValues(mock);
		verify(mock, times(2)).setLong(1, 42L);
		verify(mock, times(2)).setLong(2, 43L);
	}
	
}