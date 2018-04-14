package org.gama.stalactite.command.builder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.collection.Maps;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.command.builder.UpdateCommandBuilder.UpdateStatement;
import org.gama.stalactite.command.model.Update;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operand;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class UpdateCommandBuilderTest {
	
	@Test
	public void testToSQL_singleTable() {
		Table totoTable = new Table("Toto");
		Column<String> columnA = totoTable.addColumn("a", String.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		
		Update update = new Update(totoTable)
				.set(columnA)
				.set(columnB);
		update.where(columnA, Operand.eq(44)).or(columnA, Operand.eq(columnB));
		UpdateCommandBuilder testInstance = new UpdateCommandBuilder(update);
		assertEquals("update Toto set a = ?, b = ? where a = 44 or a = b", testInstance.toSQL());
		
		update = new Update(totoTable)
				.set(columnA, columnB);
		testInstance = new UpdateCommandBuilder(update);
		assertEquals("update Toto set a = b", testInstance.toSQL());
		
		
		update = new Update(totoTable)
				.set(columnA, "tata");
		testInstance = new UpdateCommandBuilder(update);
		assertEquals("update Toto set a = 'tata'", testInstance.toSQL());
	}
	
	@Test
	public void testToSQL_multiTable() {
		Table totoTable = new Table("Toto");
		Column<String> columnA = totoTable.addColumn("a", String.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		Table tataTable = new Table("Tata");
		Column<Long> columnX = tataTable.addColumn("x", Long.class);
		Column<String> columnY = tataTable.addColumn("y", String.class);
		
		Update update = new Update(totoTable)
				.set(columnA)
				.set(columnB);
		update.where(columnA, Operand.eq(columnX)).or(columnA, Operand.eq(columnY));
		UpdateCommandBuilder testInstance = new UpdateCommandBuilder(update);
		assertEquals("update Toto, Tata set Toto.a = ?, Toto.b = ? where Toto.a = Tata.x or Toto.a = Tata.y", testInstance.toSQL());
		
		update = new Update(totoTable)
				.set(columnA, columnB);
		update.where(columnA, Operand.eq(columnX)).or(columnA, Operand.eq(columnY));
		testInstance = new UpdateCommandBuilder(update);
		assertEquals("update Toto, Tata set Toto.a = Toto.b where Toto.a = Tata.x or Toto.a = Tata.y", testInstance.toSQL());
	}
	
	@Test
	public void testToStatement() throws SQLException {
		Table totoTable = new Table("Toto");
		Column<Long> columnA = totoTable.addColumn("a", Long.class);
		Column<Long> columnB = totoTable.addColumn("b", Long.class);
		Column<String> columnC = totoTable.addColumn("c", String.class);
		Column<String> columnD = totoTable.addColumn("d", String.class);
		
		Update update = new Update(totoTable)
				.set(columnA)
				.set(columnB, columnA)
				.set(columnC, "tata")
				.set(columnD);
		update.where(columnA, Operand.in(42L, 43L)).or(columnA, Operand.eq(columnB));
		UpdateCommandBuilder testInstance = new UpdateCommandBuilder(update);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
		
		UpdateStatement result = testInstance.toStatement(binderRegistry);
		assertEquals("update Toto set a = ?, b = a, c = ?, d = ? where a in (?, ?) or a = b", result.getSQL());
				
		assertEquals(Maps.asMap(2, (Object) "tata").add(4, 42L).add(5, 43L), result.getValues());
		assertEquals(DefaultParameterBinders.LONG_BINDER, result.getParameterBinder(1));
		assertEquals(DefaultParameterBinders.STRING_BINDER, result.getParameterBinder(2));
		assertEquals(DefaultParameterBinders.STRING_BINDER, result.getParameterBinder(3));
		assertEquals(DefaultParameterBinders.LONG_BINDER, result.getParameterBinder(4));
		assertEquals(DefaultParameterBinders.LONG_BINDER, result.getParameterBinder(5));
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
		assertEquals(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, result.getParameterBinder(1));
		result.setValue(columnA, -42l);
		result.setValue(columnD, "toto");
		result.applyValues(mock);
		verify(mock).setLong(1, -42l);
	}
}