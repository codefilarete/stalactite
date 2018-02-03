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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
		
		Update update = new Update(totoTable)
				.set(columnA)
				.set(columnB, columnA)
				.set(columnC, "tata");
		update.where(columnA, Operand.in(42, 43)).or(columnA, Operand.eq(columnB));
		UpdateCommandBuilder testInstance = new UpdateCommandBuilder(update);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
		binderRegistry.register(columnA, DefaultParameterBinders.STRING_BINDER);
		binderRegistry.register(columnB, DefaultParameterBinders.STRING_BINDER);
		
		UpdateStatement result = testInstance.toStatement(binderRegistry);
		assertEquals("update Toto set a = ?, b = a, c = ? where a in (?, ?) or a = b", result.getSQL());
				
		assertEquals(Maps.asMap(2, (Object) "tata").add(3, 42).add(4, 43), result.getValues());
		assertEquals(DefaultParameterBinders.STRING_BINDER, result.getParameterBinder(2));
		assertEquals(DefaultParameterBinders.INTEGER_BINDER, result.getParameterBinder(3));
		assertEquals(DefaultParameterBinders.INTEGER_BINDER, result.getParameterBinder(4));
		result.setValue(columnA, "Hello");
		
		PreparedStatement mock = mock(PreparedStatement.class);
		result.applyValues(mock);
		verify(mock).setString(1, "Hello");
		verify(mock).setString(2, "tata");
		verify(mock).setInt(3, 42);
		verify(mock).setInt(4, 43);
	}
}