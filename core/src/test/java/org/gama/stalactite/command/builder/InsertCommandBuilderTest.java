package org.gama.stalactite.command.builder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.collection.Maps;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.command.builder.InsertCommandBuilder.InsertStatement;
import org.gama.stalactite.command.model.Insert;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class InsertCommandBuilderTest {
	
	@Test
	public void testToSQL() {
		Table totoTable = new Table("Toto");
		Column<Long> columnA = totoTable.addColumn("a", Long.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		Insert insert = new Insert(totoTable)
				.set(columnA)
				.set(columnB, "tata");
		
		InsertCommandBuilder testInstance = new InsertCommandBuilder(insert);
		
		assertEquals("insert into Toto(a, b) values (?, 'tata')", testInstance.toSQL());
	}
	
	@Test
	public void testToStatement() throws SQLException {
		Table totoTable = new Table("Toto");
		Column<Long> columnA = totoTable.addColumn("a", Long.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		Column<String> columnC = totoTable.addColumn("c", String.class);
		Insert insert = new Insert(totoTable)
				.set(columnA)
				.set(columnB, "tata")
				.set(columnC);
		
		InsertCommandBuilder testInstance = new InsertCommandBuilder(insert);
		
		ColumnBinderRegistry binderRegistry = new ColumnBinderRegistry();
//		binderRegistry.register(columnA, DefaultParameterBinders.INTEGER_BINDER);
//		binderRegistry.register(columnB, DefaultParameterBinders.STRING_BINDER);
//		binderRegistry.register(columnC, DefaultParameterBinders.STRING_BINDER);
		
		InsertStatement result = testInstance.toStatement(binderRegistry);
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", result.getSQL());
		
		assertEquals(Maps.asMap(2, (Object) "tata"), result.getValues());
		assertEquals(DefaultParameterBinders.LONG_BINDER, result.getParameterBinder(1));
		assertEquals(DefaultParameterBinders.STRING_BINDER, result.getParameterBinder(2));
		assertEquals(DefaultParameterBinders.STRING_BINDER, result.getParameterBinder(3));
		
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
		assertEquals(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, result.getParameterBinder(1));
		result.setValue(columnA, -42l);
		result.setValue(columnC, "toto");
		result.applyValues(mock);
		verify(mock).setLong(1, -42l);

	}
	
}