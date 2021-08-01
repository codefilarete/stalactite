package org.gama.stalactite.persistence.sql.dml;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator.NoopSorter;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
		assertThat(builtInsert.getSQL()).isEqualTo("insert into Toto(A, B) values (?, ?)");
		
		assertThat(builtInsert.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtInsert.getIndexes(colB)[0]).isEqualTo(2);
		assertThat(builtInsert.getParameterBinder(colA)).isEqualTo(stringBinder);
		assertThat(builtInsert.getParameterBinder(colB)).isEqualTo(stringBinder);
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
		assertThat(builtInsert.getSQL()).isEqualTo("insert into Toto('key', B) values (?, ?)");
		
		assertThat(builtInsert.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtInsert.getIndexes(colB)[0]).isEqualTo(2);
		assertThat(builtInsert.getParameterBinder(colA)).isEqualTo(stringBinder);
		assertThat(builtInsert.getParameterBinder(colB)).isEqualTo(stringBinder);
	}
	
	@Test
	public void testBuildUpdate() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		PreparedUpdate builtUpdate = testInstance.buildUpdate(toto.getColumns(), Arrays.asList(colA));
		assertThat(builtUpdate.getSQL()).isEqualTo("update Toto set A = ?, B = ? where A = ?");

		assertThat(builtUpdate.getIndex(new UpwhereColumn<>(colA, true))).isEqualTo(1);
		assertThat(builtUpdate.getIndex(new UpwhereColumn<>(colB, true))).isEqualTo(2);
		assertThat(builtUpdate.getIndex(new UpwhereColumn<>(colA, false))).isEqualTo(3);
		
		assertThat(builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, true))).isEqualTo(stringBinder);
		assertThat(builtUpdate.getParameterBinder(new UpwhereColumn<>(colB, true))).isEqualTo(stringBinder);
		assertThat(builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, false))).isEqualTo(stringBinder);
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
		assertThat(builtUpdate.getSQL()).isEqualTo("update Toto set 'key' = ?, B = ? where 'key' = ? and B = ?");

		assertThat(builtUpdate.getIndex(new UpwhereColumn<>(colA, true))).isEqualTo(1);
		assertThat(builtUpdate.getIndex(new UpwhereColumn<>(colB, true))).isEqualTo(2);
		assertThat(builtUpdate.getIndex(new UpwhereColumn<>(colA, false))).isEqualTo(3);
		
		assertThat(builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, true))).isEqualTo(stringBinder);
		assertThat(builtUpdate.getParameterBinder(new UpwhereColumn<>(colB, true))).isEqualTo(stringBinder);
		assertThat(builtUpdate.getParameterBinder(new UpwhereColumn<>(colA, false))).isEqualTo(stringBinder);
	}
	
	@Test
	public void testBuildDelete() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		ColumnParameterizedSQL builtDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where A = ?");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
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
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where 'key' = ?");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void testBuildMassiveDelete() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		Collection<Column<Table, Object>> keys = (Collection<Column<Table, Object>>) (Collection) Collections.singleton(colA);
		ColumnParameterizedSQL<Table> builtDelete = testInstance.buildDeleteByKey(toto, keys, 2);
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where A in (?, ?)");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void testBuildMassiveDelete_multipleKeys() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		toto.addColumn("C", String.class);
		
		Collection<Column<Table, Object>> keys = (Collection<Column<Table, Object>>) (Collection) Arrays.asList(colA, colB);
		ColumnParameterizedSQL<Table> builtDelete = testInstance.buildDeleteByKey(toto, keys, 3);
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where (A, B) in ((?, ?), (?, ?), (?, ?))");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
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
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where 'key' in (?)");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void testBuildSelect() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		ColumnParameterizedSQL builtSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA, colB));
		assertThat(builtSelect.getSQL()).isEqualTo("select A, B from Toto where A = ? and B = ?");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
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
		assertThat(builtSelect.getSQL()).isEqualTo("select 'key', B from Toto where 'key' = ? and B = ?");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void testBuildMassiveSelect() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		Iterable<Column<Table, Object>> selection = Arrays.asList(colA, colB);
		Set<Column<Table, Object>> keys = Collections.singleton(colA);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, selection, keys, 5);
		assertThat(builtSelect.getSQL()).isEqualTo("select A, B from Toto where A in (?, ?, ?, ?, ?)");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	
	@Test
	public void testBuildMassiveSelect_multipleKeys() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		Iterable<Column<Table, Object>> selection = Arrays.asList(colA, colB);
		Set<Column<Table, Object>> keys = Arrays.asSet(colA, colB);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, selection, keys, 3);
		assertThat(builtSelect.getSQL()).isEqualTo("select A, B from Toto where (A, B) in ((?, ?), (?, ?), (?, ?))");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
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
		assertThat(builtSelect.getSQL()).isEqualTo("select 'key', B from Toto where 'key' in (?, ?, ?, ?, ?)");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	
}