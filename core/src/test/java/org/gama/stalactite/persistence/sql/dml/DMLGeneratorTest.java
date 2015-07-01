package org.gama.stalactite.persistence.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DMLGeneratorTest {

	private ParameterBinder stringBinder;
	private DMLGenerator testInstance;
	
	@BeforeMethod
	public void setUp() {
		// Nécessaire aux CRUDOperations et RowIterator qui ont besoin du ParamaterBinderRegistry
		Dialect currentDialect = new Dialect(new JavaTypeToSqlTypeMapping());
		PersistenceContext.setCurrent(new PersistenceContext(null, currentDialect));
		stringBinder = currentDialect.getColumnBinderRegistry().getBinder(String.class);
		testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry());
	}
	
	@Test
	public void testBuildInsert() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);

		ColumnPreparedSQL buildedInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals(buildedInsert.getSQL(), "insert into Toto(A, B) values (?, ?)");
		
		assertEquals(1, buildedInsert.getIndexes(colA)[0]);
		assertEquals(2, buildedInsert.getIndexes(colB)[0]);
		assertEquals(stringBinder, buildedInsert.getParameterBinder(colA));
		assertEquals(stringBinder, buildedInsert.getParameterBinder(colB));
	}
	
	@Test
	public void testBuildUpdate() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		PreparedUpdate buildedUpdate = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA));
		assertEquals(buildedUpdate.getSQL(), "update Toto set A = ?, B = ? where A = ?");

		assertEquals(1, buildedUpdate.getIndex(new UpwhereColumn(colA, true)));
		assertEquals(2, buildedUpdate.getIndex(new UpwhereColumn(colB, true)));
		assertEquals(3, buildedUpdate.getIndex(new UpwhereColumn(colA, false)));
		
		assertEquals(stringBinder, buildedUpdate.getParameterBinder(new UpwhereColumn(colA, true)));
		assertEquals(stringBinder, buildedUpdate.getParameterBinder(new UpwhereColumn(colB, true)));
		assertEquals(stringBinder, buildedUpdate.getParameterBinder(new UpwhereColumn(colA, false)));
	}
	
	@Test
	public void testBuildDelete() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		ColumnPreparedSQL buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals(buildedDelete.getSQL(), "delete Toto where A = ?");
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveDelete() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		toto.new Column("B", String.class);
		
		ColumnPreparedSQL buildedDelete = testInstance.buildMassiveDelete(toto, colA, 1);
		assertEquals(buildedDelete.getSQL(), "delete Toto where A in (?)");
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildSelect() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		ColumnPreparedSQL buildedSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA));
		assertEquals(buildedSelect.getSQL(), "select A, B from Toto where A = ?");
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveSelect() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.new Column("A", String.class);
		Column colB = toto.new Column("B", String.class);
		
		ColumnPreparedSQL buildedSelect = testInstance.buildMassiveSelect(toto, Arrays.asList(colA, colB), colA, 5);
		assertEquals(buildedSelect.getSQL(), "select A, B from Toto where A in (?, ?, ?, ?, ?)");
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
}