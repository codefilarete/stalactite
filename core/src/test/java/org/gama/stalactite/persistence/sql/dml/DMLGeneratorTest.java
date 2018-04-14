package org.gama.stalactite.persistence.sql.dml;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;

import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator.NoopSorter;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.DMLNameProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DMLGeneratorTest {

	private ParameterBinder stringBinder;
	private DMLGenerator testInstance;
	private Dialect currentDialect;
	
	@BeforeEach
	public void setUp() {
		currentDialect = new Dialect(new JavaTypeToSqlTypeMapping());
		stringBinder = currentDialect.getColumnBinderRegistry().getBinder(String.class);
		testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE);
	}
	
	@Test
	public void testBuildInsert() {
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
	public void testBuildInsert_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, dmlNameProvider);

		ColumnParamedSQL buildedInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals(buildedInsert.getSQL(), "insert into Toto('key', B) values (?, ?)");
		
		assertEquals(1, buildedInsert.getIndexes(colA)[0]);
		assertEquals(2, buildedInsert.getIndexes(colB)[0]);
		assertEquals(stringBinder, buildedInsert.getParameterBinder(colA));
		assertEquals(stringBinder, buildedInsert.getParameterBinder(colB));
	}
	
	@Test
	public void testBuildUpdate() {
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
	public void testBuildUpdate_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, dmlNameProvider);
		
		PreparedUpdate buildedUpdate = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA, colB));
		assertEquals(buildedUpdate.getSQL(), "update Toto set 'key' = ?, B = ? where 'key' = ? and B = ?");

		assertEquals(1, buildedUpdate.getIndex(new UpwhereColumn(colA, true)));
		assertEquals(2, buildedUpdate.getIndex(new UpwhereColumn(colB, true)));
		assertEquals(3, buildedUpdate.getIndex(new UpwhereColumn(colA, false)));
		
		assertEquals(stringBinder, buildedUpdate.getParameterBinder(new UpwhereColumn(colA, true)));
		assertEquals(stringBinder, buildedUpdate.getParameterBinder(new UpwhereColumn(colB, true)));
		assertEquals(stringBinder, buildedUpdate.getParameterBinder(new UpwhereColumn(colA, false)));
	}
	
	@Test
	public void testBuildDelete() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals("delete from Toto where A = ?", buildedDelete.getSQL());
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildDelete_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, dmlNameProvider);
		
		ColumnParamedSQL buildedDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals("delete from Toto where 'key' = ?", buildedDelete.getSQL());
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveDelete() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedDelete = testInstance.buildMassiveDelete(toto, colA, 2);
		assertEquals("delete from Toto where A in (?, ?)", buildedDelete.getSQL());
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveDelete_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, dmlNameProvider);
		
		ColumnParamedSQL buildedDelete = testInstance.buildMassiveDelete(toto, colA, 1);
		assertEquals("delete from Toto where 'key' in (?)", buildedDelete.getSQL());
		
		assertEquals(1, buildedDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildSelect() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA, colB));
		assertEquals("select A, B from Toto where A = ? and B = ?", buildedSelect.getSQL());
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildSelect_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("key", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, dmlNameProvider);
		
		ColumnParamedSQL buildedSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA, colB));
		assertEquals("select 'key', B from Toto where 'key' = ? and B = ?", buildedSelect.getSQL());
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveSelect() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		ColumnParamedSQL buildedSelect = testInstance.buildMassiveSelect(toto, Arrays.asList(colA, colB), colA, 5);
		assertEquals("select A, B from Toto where A in (?, ?, ?, ?, ?)", buildedSelect.getSQL());
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveSelect_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, dmlNameProvider);
		
		ColumnParamedSQL buildedSelect = testInstance.buildMassiveSelect(toto, Arrays.asList(colA, colB), colA, 5);
		assertEquals("select 'key', B from Toto where 'key' in (?, ?, ?, ?, ?)", buildedSelect.getSQL());
		
		assertEquals(1, buildedSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, buildedSelect.getParameterBinder(colA));
	}
	
	
}