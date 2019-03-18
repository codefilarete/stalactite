package org.gama.stalactite.persistence.sql.dml;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator.NoopSorter;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
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

		ColumnParameterizedSQL builtInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals("insert into Toto(A, B) values (?, ?)", builtInsert.getSQL());
		
		assertEquals(1, builtInsert.getIndexes(colA)[0]);
		assertEquals(2, builtInsert.getIndexes(colB)[0]);
		assertEquals(stringBinder, builtInsert.getParameterBinder(colA));
		assertEquals(stringBinder, builtInsert.getParameterBinder(colB));
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

		ColumnParameterizedSQL builtInsert = testInstance.buildInsert(toto.getColumns());
		assertEquals("insert into Toto('key', B) values (?, ?)", builtInsert.getSQL());
		
		assertEquals(1, builtInsert.getIndexes(colA)[0]);
		assertEquals(2, builtInsert.getIndexes(colB)[0]);
		assertEquals(stringBinder, builtInsert.getParameterBinder(colA));
		assertEquals(stringBinder, builtInsert.getParameterBinder(colB));
	}
	
	@Test
	public void testBuildUpdate() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		PreparedUpdate builtUpdate = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA));
		assertEquals("update Toto set A = ?, B = ? where A = ?", builtUpdate.getSQL());

		assertEquals(1, builtUpdate.getIndex(new UpwhereColumn<>(colA, true)));
		assertEquals(2, builtUpdate.getIndex(new UpwhereColumn<>(colB, true)));
		assertEquals(3, builtUpdate.getIndex(new UpwhereColumn<>(colA, false)));
		
		assertEquals(stringBinder, builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, true)));
		assertEquals(stringBinder, builtUpdate.getParameterBinder(new UpwhereColumn<>(colB, true)));
		assertEquals(stringBinder, builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, false)));
	}
	
	@Test
	public void testBuildUpdate_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
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
		
		PreparedUpdate builtUpdate = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA, colB));
		assertEquals("update Toto set 'key' = ?, B = ? where 'key' = ? and B = ?", builtUpdate.getSQL());

		assertEquals(1, builtUpdate.getIndex(new UpwhereColumn<>(colA, true)));
		assertEquals(2, builtUpdate.getIndex(new UpwhereColumn<>(colB, true)));
		assertEquals(3, builtUpdate.getIndex(new UpwhereColumn<>(colA, false)));
		
		assertEquals(stringBinder, builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, true)));
		assertEquals(stringBinder, builtUpdate.getParameterBinder(new UpwhereColumn<>(colB, true)));
		assertEquals(stringBinder, builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, false)));
	}
	
	@Test
	public void testBuildDelete() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		ColumnParameterizedSQL builtDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals("delete from Toto where A = ?", builtDelete.getSQL());
		
		assertEquals(1, builtDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildDelete_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
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
		
		ColumnParameterizedSQL builtDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertEquals("delete from Toto where 'key' = ?", builtDelete.getSQL());
		
		assertEquals(1, builtDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveDelete() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		Collection<Column<Table, Object>> keys = (Collection<Column<Table, Object>>) (Collection) Collections.singleton(colA);
		ColumnParameterizedSQL<Table> builtDelete = testInstance.buildDeleteByKey(toto, keys, 2);
		assertEquals("delete from Toto where A in (?, ?)", builtDelete.getSQL());
		
		assertEquals(1, builtDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveDelete_multipleKeys() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		toto.addColumn("C", String.class);
		
		Collection<Column<Table, Object>> keys = (Collection<Column<Table, Object>>) (Collection) Arrays.asList(colA, colB);
		ColumnParameterizedSQL<Table> builtDelete = testInstance.buildDeleteByKey(toto, keys, 3);
		assertEquals("delete from Toto where (A, B) in ((?, ?), (?, ?), (?, ?))", builtDelete.getSQL());
		
		assertEquals(1, builtDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveDelete_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
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
		
		Set<Column<Table, Object>> singleton = (Set) Collections.singleton(colA);
		ColumnParameterizedSQL builtDelete = testInstance.buildDeleteByKey(toto, singleton, 1);
		assertEquals("delete from Toto where 'key' in (?)", builtDelete.getSQL());
		
		assertEquals(1, builtDelete.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtDelete.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildSelect() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		ColumnParameterizedSQL builtSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA, colB));
		assertEquals("select A, B from Toto where A = ? and B = ?", builtSelect.getSQL());
		
		assertEquals(1, builtSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildSelect_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("key", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
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
		
		ColumnParameterizedSQL builtSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA, colB));
		assertEquals("select 'key', B from Toto where 'key' = ? and B = ?", builtSelect.getSQL());
		
		assertEquals(1, builtSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveSelect() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		Iterable<Column<Table, Object>> selection = Arrays.asList(colA, colB);
		Set<Column<Table, Object>> keys = Collections.singleton(colA);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, selection, keys, 5);
		assertEquals("select A, B from Toto where A in (?, ?, ?, ?, ?)", builtSelect.getSQL());
		
		assertEquals(1, builtSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtSelect.getParameterBinder(colA));
	}
	
	
	@Test
	public void testBuildMassiveSelect_multipleKeys() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		Iterable<Column<Table, Object>> selection = Arrays.asList(colA, colB);
		Set<Column<Table, Object>> keys = Arrays.asSet(colA, colB);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, selection, keys, 3);
		assertEquals("select A, B from Toto where (A, B) in ((?, ?), (?, ?), (?, ?))", builtSelect.getSQL());
		
		assertEquals(1, builtSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtSelect.getParameterBinder(colA));
	}
	
	@Test
	public void testBuildMassiveSelect_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
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
		
		Set<Column<Table, Object>> keys = Collections.singleton(colA);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, Arrays.asList(colA, colB), keys, 5);
		assertEquals("select 'key', B from Toto where 'key' in (?, ?, ?, ?, ?)", builtSelect.getSQL());
		
		assertEquals(1, builtSelect.getIndexes(colA)[0]);
		assertEquals(stringBinder, builtSelect.getParameterBinder(colA));
	}
	
	
}