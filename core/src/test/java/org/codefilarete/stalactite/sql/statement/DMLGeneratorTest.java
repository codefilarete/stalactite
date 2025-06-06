package org.codefilarete.stalactite.sql.statement;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DMLGeneratorTest {

	private ParameterBinder stringBinder;
	private DMLGenerator testInstance;
	private Dialect currentDialect;
	
	@BeforeEach
	public void setUp() {
		currentDialect = new DefaultDialect(new JavaTypeToSqlTypeMapping());
		stringBinder = currentDialect.getColumnBinderRegistry().getBinder(String.class);
		testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, DMLNameProvider::new);
	}
	
	@Test
	public void buildInsert() {
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
	public void buildInsert_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(Selectable<?> column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, k -> dmlNameProvider);

		ColumnParameterizedSQL builtInsert = testInstance.buildInsert(toto.getColumns());
		assertThat(builtInsert.getSQL()).isEqualTo("insert into Toto('key', B) values (?, ?)");
		
		assertThat(builtInsert.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtInsert.getIndexes(colB)[0]).isEqualTo(2);
		assertThat(builtInsert.getParameterBinder(colA)).isEqualTo(stringBinder);
		assertThat(builtInsert.getParameterBinder(colB)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildUpdate() {
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
	public void buildUpdate_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		Column<Table, Object> colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(Selectable<?> column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, k -> dmlNameProvider);
		
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
	public void buildDelete() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		ColumnParameterizedSQL builtDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where A = ?");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildDelete_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, Object> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(Selectable<?> column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, k -> dmlNameProvider);
		
		ColumnParameterizedSQL builtDelete = testInstance.buildDelete(toto, Arrays.asList(colA));
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where 'key' = ?");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildMassiveDelete() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		Collection<Column<Table, ?>> keys = (Collection<Column<Table, ?>>) (Collection) Collections.singleton(colA);
		ColumnParameterizedSQL builtDelete = testInstance.buildDeleteByKey(toto, keys, 2);
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where A in (?, ?)");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildMassiveDelete_multipleKeys() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		toto.addColumn("C", String.class);
		
		Collection<Column<Table, ?>> keys = (Collection<Column<Table, ?>>) (Collection) Arrays.asList(colA, colB);
		ColumnParameterizedSQL builtDelete = testInstance.buildDeleteByKey(toto, keys, 3);
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where (A, B) in ((?, ?), (?, ?), (?, ?))");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildMassiveDelete_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(Selectable<?> column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, k -> dmlNameProvider);
		
		Set<Column<Table, ?>> singleton = Collections.singleton(colA);
		ColumnParameterizedSQL builtDelete = testInstance.buildDeleteByKey(toto, singleton, 1);
		assertThat(builtDelete.getSQL()).isEqualTo("delete from Toto where 'key' in (?)");
		
		assertThat(builtDelete.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtDelete.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildSelect() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		
		ColumnParameterizedSQL builtSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA, colB));
		assertThat(builtSelect.getSQL()).isEqualTo("select A, B from Toto where A = ? and B = ?");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildSelect_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("key", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(Selectable<?> column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, k -> dmlNameProvider);
		
		ColumnParameterizedSQL builtSelect = testInstance.buildSelect(toto, Arrays.asList(colA, colB), Arrays.asList(colA, colB));
		assertThat(builtSelect.getSQL()).isEqualTo("select 'key', B from Toto where 'key' = ? and B = ?");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildMassiveSelect() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		
		Iterable<Column<Table, ?>> selection = Arrays.asList(colA, colB);
		Set<Column<Table, ?>> keys = Collections.singleton(colA);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, selection, keys, 5);
		assertThat(builtSelect.getSQL()).isEqualTo("select A, B from Toto where A in (?, ?, ?, ?, ?)");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	
	@Test
	public void buildMassiveSelect_multipleKeys() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		
		Iterable<Column<Table, ?>> selection = Arrays.asList(colA, colB);
		Set<Column<Table, ?>> keys = Arrays.asSet(colA, colB);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, selection, keys, 3);
		assertThat(builtSelect.getSQL()).isEqualTo("select A, B from Toto where (A, B) in ((?, ?), (?, ?), (?, ?))");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	@Test
	public void buildMassiveSelect_dmlNameProviderUsed() {
		Table toto = new Table(null, "Toto");
		Column<Table, String> colA = toto.addColumn("A", String.class);
		Column<Table, String> colB = toto.addColumn("B", String.class);
		
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(Selectable<?> column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		DMLGenerator testInstance = new DMLGenerator(currentDialect.getColumnBinderRegistry(), NoopSorter.INSTANCE, k -> dmlNameProvider);
		
		Set<Column<Table, ?>> keys = Collections.singleton(colA);
		ColumnParameterizedSQL builtSelect = testInstance.buildSelectByKey(toto, Arrays.asList(colA, colB), keys, 5);
		assertThat(builtSelect.getSQL()).isEqualTo("select 'key', B from Toto where 'key' in (?, ?, ?, ?, ?)");
		
		assertThat(builtSelect.getIndexes(colA)[0]).isEqualTo(1);
		assertThat(builtSelect.getParameterBinder(colA)).isEqualTo(stringBinder);
	}
	
	
}