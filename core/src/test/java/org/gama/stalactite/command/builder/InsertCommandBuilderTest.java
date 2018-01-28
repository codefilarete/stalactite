package org.gama.stalactite.command.builder;

import org.gama.lang.collection.Maps;
import org.gama.lang.collection.Maps.ChainingMap;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.command.model.Insert;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
				.set(columnB);
		
		InsertCommandBuilder testInstance = new InsertCommandBuilder(insert);
		
		assertEquals("insert into Toto(a, b) values (?, ?)", testInstance.toSQL());
	}
	
	@Test
	public void testToStatement() {
		Table totoTable = new Table("Toto");
		Column<Long> columnA = totoTable.addColumn("a", Long.class);
		Column<String> columnB = totoTable.addColumn("b", String.class);
		Insert insert = new Insert(totoTable)
				.set(columnA)
				.set(columnB);
		
		InsertCommandBuilder testInstance = new InsertCommandBuilder(insert);
		
		ChainingMap<Column, ParameterBinder> parameterBinderMap = Maps
				.asMap((Column) columnA, (ParameterBinder) DefaultParameterBinders.STRING_BINDER)
				.add(columnA, DefaultParameterBinders.STRING_BINDER);
		ColumnParamedSQL result = testInstance.toStatement(ParameterBinderIndex.fromMap(parameterBinderMap));
		assertEquals("insert into Toto(a, b) values (?, ?)", result.getSQL());
		assertArrayEquals(new int[] { 1 }, result.getIndexes(columnA));
		assertArrayEquals(new int[] { 2 }, result.getIndexes(columnB));
	}
	
}