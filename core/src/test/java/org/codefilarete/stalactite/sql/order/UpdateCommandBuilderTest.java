package org.codefilarete.stalactite.sql.order;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder.UpdateStatement;
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
	<T extends Table<T>> void toSQL_singleTable() {
		T totoTable = (T) new Table("Toto");
		Column<T, String> columnA = totoTable.addColumn("a", String.class);
		Column<T, String> columnB = totoTable.addColumn("b", String.class);
		
		Update<T> update = new Update<>(totoTable)
				.set(columnA)
				.set(columnB);
		update.where(columnA, Operators.eq(44)).or(columnA, Operators.eq(columnB));
		Dialect dialect = new Dialect();
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<>(update, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = ?, b = ? where a = 44 or a = b");
		
		update = new Update<>(totoTable)
				.set(columnA, columnB);
		testInstance = new UpdateCommandBuilder<>(update, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = b");
		
		
		update = new Update<>(totoTable)
				.set(columnA, "tata");
		testInstance = new UpdateCommandBuilder<>(update, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto set a = 'tata'");
	}
	
	@Test
	<T1 extends Table<T1>, T2 extends Table<T2>> void toSQL_multiTable() {
		T1 totoTable = (T1) new Table("Toto");
		Column<T1, String> columnA = totoTable.addColumn("a", String.class);
		Column<T1, String> columnB = totoTable.addColumn("b", String.class);
		Column<T1, String> columnC = totoTable.addColumn("c", String.class);
		T2 tataTable = (T2) new Table("Tata");
		Column<T2, Long> columnX = tataTable.addColumn("x", Long.class);
		Column<T2, String> columnY = tataTable.addColumn("y", String.class);
		Column<T2, String> columnZ = tataTable.addColumn("z", String.class);
		
		
		Update<T1> update1 = new Update<>(totoTable)
				.set(columnA)
				.set(columnB);
		update1.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		Dialect dialect = new Dialect();
		UpdateCommandBuilder<T1> testInstance = new UpdateCommandBuilder<>(update1, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto, Tata set Toto.a = ?, Toto.b = ? where Toto.a = Tata.x or Toto.a = Tata.y");
		
		Update<T1> update2 = new Update<>(totoTable)
				.set(columnC, columnZ);
		update2.where(columnA, Operators.eq(columnX)).or(columnA, Operators.eq(columnY));
		testInstance = new UpdateCommandBuilder<>(update2, dialect);
		assertThat(testInstance.toSQL()).isEqualTo("update Toto, Tata set Toto.c = Tata.z where Toto.a = Tata.x or Toto.a = Tata.y");
	}
	
	@Test
	<T extends Table<T>> void toStatement() throws SQLException {
		T totoTable = (T) new Table("Toto");
		Column<T, Long> columnA = totoTable.addColumn("a", Long.class);
		Column<T, Long> columnB = totoTable.addColumn("b", Long.class);
		Column<T, String> columnC = totoTable.addColumn("c", String.class);
		Column<T, String> columnD = totoTable.addColumn("d", String.class);
		
		Update<T> update = new Update<>(totoTable)
				.set(columnA)
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD);
		update.where(columnA, Operators.in(42L, 43L)).or(columnA, Operators.eq(columnB));
		Dialect dialect = new Dialect();
		UpdateCommandBuilder<T> testInstance = new UpdateCommandBuilder<T>(update, dialect);
		
		UpdateStatement<T> result = testInstance.toStatement();
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
		dialect.getColumnBinderRegistry().register(columnA, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result = testInstance.toStatement();
		assertThat(result.getParameterBinder(1)).isEqualTo(DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result.setValue(columnA, -42l);
		result.setValue(columnD, "toto");
		result.applyValues(mock);
		verify(mock).setLong(1, -42l);
	}
}