package org.gama.stalactite.persistence.sql.dml;

import org.gama.lang.collection.Maps;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class DMLGeneratorTest {

	private ParameterBinder stringBinder;

	@BeforeMethod
	public void setUp() {
		// Nécessaire aux CRUDOperations et RowIterator qui ont besoin du ParamaterBinderRegistry
		Dialect currentDialect = new Dialect(new JavaTypeToSqlTypeMapping());
		PersistenceContext.setCurrent(new PersistenceContext(null, currentDialect));
		stringBinder = currentDialect.getColumnBinderRegistry().getBinder(String.class);
	}
	
	@Test
	public void testBuildInsert() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);

		DMLGenerator testInstance = new DMLGenerator();
		InsertOperation buildedInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals(buildedInsert.getSql(), "insert into Toto(A, B) values (?, ?)");

		Map<Column, Map.Entry<Integer, ParameterBinder>> upsertIndexes = buildedInsert.getInsertIndexes();
		assertEquals(upsertIndexes, Maps.asMap(colA, new AbstractMap.SimpleEntry<>(1, stringBinder))
									.add(colB, new AbstractMap.SimpleEntry<>(2, stringBinder)));
	}
	
	@Test
	public void testBuildUpdate() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		UpdateOperation buildedUpdate = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA));
		assertEquals(buildedUpdate.getSql(), "update Toto set A = ?, B = ? where A = ?");

		Map<Column, Map.Entry<Integer, ParameterBinder>> upsertIndexes = buildedUpdate.getUpdateIndexes();
		assertEquals(upsertIndexes, Maps.asMap(colA, new AbstractMap.SimpleEntry<>(1, stringBinder))
									.add(colB, new AbstractMap.SimpleEntry<>(2, stringBinder)));
		
		Map<Column, Map.Entry<Integer, ParameterBinder>> whereIndexes = buildedUpdate.getWhereIndexes();
		assertEquals(whereIndexes, Maps.asMap(colA, new AbstractMap.SimpleEntry<>(3, stringBinder)));
	}
	
	@Test
	public void testBuildDelete() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		DeleteOperation buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals(buildedDelete.getSql(), "delete Toto where A = ?");
		
		Map<Column, Map.Entry<Integer, ParameterBinder>> whereIndexes = buildedDelete.getWhereIndexes();
		assertEquals(whereIndexes, Maps.asMap(colA, new AbstractMap.SimpleEntry<>(1, stringBinder)));
	}
	
	@Test
	public void testBuildSelect() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		DMLGenerator testInstance = new DMLGenerator();
		SelectOperation buildedSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA));
		assertEquals(buildedSelect.getSql(), "select A, B from Toto where A = ?");
		
		Map<Column, Map.Entry<Integer, ParameterBinder>> whereIndexes = buildedSelect.getWhereIndexes();
		assertEquals(whereIndexes, Maps.asMap(colA, new AbstractMap.SimpleEntry<>(1, stringBinder)));
	}
}