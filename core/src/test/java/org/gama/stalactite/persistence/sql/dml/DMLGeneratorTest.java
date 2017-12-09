package org.gama.stalactite.persistence.sql.dml;

import java.util.Arrays;

import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DMLGeneratorTest {

	private ParameterBinder stringBinder;
	private DMLGenerator testInstance;
	
	@Before
	public void setUp() {
		// Nécessaire aux CRUDOperations et RowIterator qui ont besoin du ParamaterBinderRegistry
		Dialect currentDialect = new Dialect(new JavaTypeToSqlTypeMapping());
		stringBinder = currentDialect.getColumnBinderRegistry().getBinder(String.class);
		testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
	}
	
	@Test
	public void testBuildInsert() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);

		ColumnParamedSQL buildedInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals(buildedInsert.getSQL(), "insert into Toto(A, B) values (?, ?)");
		
		assertEquals(1, buildedInsert.getIndexes(colA)[0]);
		assertEquals(2, buildedInsert.getIndexes(colB)[0]);
		assertEquals(stringBinder, buildedInsert.getParameterBinder(colA));
		assertEquals(stringBinder, buildedInsert.getParameterBinder(colB));
	}
	
	@Test
	public void testBuildUpdate() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
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
		Column colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals("delete from Toto where A = ?", buildedDelete.getSQL());
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveDelete() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedDelete = testInstance.buildMassiveDelete(toto, colA, 1);
		assertEquals("delete from Toto where A in (?)", buildedDelete.getSQL());
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildSelect() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA));
		assertEquals(buildedSelect.getSQL(), "select A, B from Toto where A = ?");
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveSelect() throws Exception {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedSelect = testInstance.buildMassiveSelect(toto, Arrays.asList(colA, colB), colA, 5);
		assertEquals(buildedSelect.getSQL(), "select A, B from Toto where A in (?, ?, ?, ?, ?)");
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
}