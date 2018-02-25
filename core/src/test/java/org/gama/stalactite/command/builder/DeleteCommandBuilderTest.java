package org.gama.stalactite.command.builder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.collection.Maps;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operand;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
		Column<Long> columnA = totoTable.addColumn("a", Long.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		
		Delete delete = new Delete(totoTable);
		delete.where(columnA, Operand.eq(44)).or(columnA, Operand.eq(columnB));
		
		DeleteCommandBuilder testInstance = new DeleteCommandBuilder(delete);
		
		assertEquals("delete from Toto where a = 44 or a = b", testInstance.toSQL());
		
		delete = new Delete(totoTable);
		
		testInstance = new DeleteCommandBuilder(delete);
		
		assertEquals("delete from Toto", testInstance.toSQL());
		
	}
	
	@Test
	public void testToSQL_multiTable() {
		Table totoTable = new Table("Toto");
		Column<Long> columnA = totoTable.addColumn("a", Long.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		Table tataTable = new Table("Tata");
		Column<Long> columnX = tataTable.addColumn("x", Long.class);
		Column<String> columnY = tataTable.addColumn("y", String.class);
		
		Delete delete = new Delete(totoTable);
		delete.where(columnA, Operand.eq(columnX)).or(columnA, Operand.eq(columnY));
		
		DeleteCommandBuilder testInstance = new DeleteCommandBuilder(delete);
		
		assertEquals("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y", testInstance.toSQL());
		
		delete = new Delete(totoTable);
		delete.where(columnA, Operand.eq(columnX)).or(columnA, Operand.eq(columnY));
		
		testInstance = new DeleteCommandBuilder(delete);
		
		assertEquals("delete from Toto, Tata where Toto.a = Tata.x or Toto.a = Tata.y", testInstance.toSQL());
	}
	
	@Test
	public void testToStatement() throws SQLException {
		Table totoTable = new Table("Toto");
		Column<Long> columnA = totoTable.addColumn("a", Long.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		
		Delete delete = new Delete(totoTable);
		delete.where(columnA,  Operand.in(42L, 43L)).or(columnA, Operand.eq(columnB));
		
		DeleteCommandBuilder testInstance = new DeleteCommandBuilder(delete);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
		
		PreparedSQL result = testInstance.toStatement(binderRegistry);
		assertEquals("delete from Toto where a in (?, ?) or a = b", result.getSQL());
		
		assertEquals(Maps.asMap(1, 42L).add(2, 43L), result.getValues());
		assertEquals(DefaultParameterBinders.LONG_BINDER, result.getParameterBinder(1));
		assertEquals(DefaultParameterBinders.LONG_BINDER, result.getParameterBinder(2));
		
		PreparedStatement mock = mock(PreparedStatement.class);
		result.applyValues(mock);
		verify(mock).setLong(1, 42L);
		verify(mock).setLong(2, 43L);
		
		// ensuring that column type override in registry is taken into account
		binderRegistry.register(columnA, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		result = testInstance.toStatement(binderRegistry);
		assertEquals(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, result.getParameterBinder(1));
		assertEquals(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, result.getParameterBinder(2));
		result.applyValues(mock);
		verify(mock, times(2)).setLong(1, 42L);
		verify(mock, times(2)).setLong(2, 43L);
	}
	
}