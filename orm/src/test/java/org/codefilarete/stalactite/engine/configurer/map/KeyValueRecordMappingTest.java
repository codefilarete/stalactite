package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.assertj.core.presentation.StandardRepresentation;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping.KeyValueRecordIdMapping.RecordIdAssembler;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SimpleIdentifierAssembler;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeyValueRecordMappingTest {
	
	@Nested
	class RecordIdAssemblerTest {
		
		@Test
		<T extends Table<T>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		void assemble_mapEntryKeyIsASimpleProperty_recordIdContainsIdentifiers() {
			T joinTable = (T) new Table("joinTable");
			Column<T, Long> joinTableIdColumn = joinTable.addColumn("id", long.class);
			Column<T, String> entryKeyColumn = joinTable.addColumn("entryKey", String.class);
			LEFTTABLE leftTable = (LEFTTABLE) new Table("leftTable");
			Column<LEFTTABLE, Long> leftTableIdColumn = leftTable.addColumn("id", long.class);
			BiFunction<Row, ColumnedRow, String> entryKeyAssembler = (row, columnedRow) -> columnedRow.getValue(entryKeyColumn, row);
			IdentifierAssembler<Long, LEFTTABLE> sourceIdentifierAssembler = new SimpleIdentifierAssembler<>(leftTableIdColumn);
			// Since we test assemble() method there's no need of columns mapping because they are used at insertion time
			Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKey2ForeignKeyMapping = Collections.emptyMap();
			RecordIdAssembler<String, Long, T> testInstance = new RecordIdAssembler<>(
					joinTable,
					entryKeyAssembler,
					s -> {
						throw new RuntimeException("This code should be called since its used in getColumnValues()");
					},
					sourceIdentifierAssembler,
					primaryKey2ForeignKeyMapping
			);
			
			Row row = new Row();
			row.add("id", 42L);
			row.add("entryKey", "toto");
			ColumnedRow rowAliaser = new ColumnedRow(selectable -> {
				// Only leftTableIdColumn and rightTableIdColumn are expected to be read by assemble()
				// Note that they should be in final select query since they are joined on association table
				if (selectable == leftTableIdColumn) {
					return "id";
				}
				if (selectable == entryKeyColumn) {
					return "entryKey";
				}
				throw new IllegalArgumentException("Column " + selectable + " is not mapped in row");
			});
			
			RecordId<String, Long> actual = testInstance.assemble(row, rowAliaser);
			assertThat(actual)
					.withRepresentation(new StandardRepresentation() {
						@Override
						protected String fallbackToStringOf(Object object) {
							return ToStringBuilder.reflectionToString(object, new MultilineRecursiveToStringStyle());
						}
					})
					.isEqualTo(new RecordId<>(42L, "toto"));
		}
		
		@Test
		<T extends Table<T>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		void assemble_mapEntryKeyIsEntityWithSingleColumn_recordIdContainsIdentifiers() {
			T joinTable = (T) new Table("joinTable");
			Column<T, Long> joinTableIdColumn = joinTable.addColumn("id", long.class);
			Column<T, String> entryKeyColumn = joinTable.addColumn("entryKey", String.class);
			LEFTTABLE leftTable = (LEFTTABLE) new Table("leftTable");
			Column<LEFTTABLE, Long> leftTableIdColumn = leftTable.addColumn("id", long.class);
			RIGHTTABLE rightTable = (RIGHTTABLE) new Table("rightTable");
			Column<RIGHTTABLE, String> rightTableIdColumn = rightTable.addColumn("entryKey", String.class);
			IdentifierAssembler<String, RIGHTTABLE> entryKeyIdentifierAssembler = new SimpleIdentifierAssembler<>(rightTableIdColumn);
			IdentifierAssembler<Long, LEFTTABLE> sourceIdentifierAssembler = new SimpleIdentifierAssembler<>(leftTableIdColumn);
			// Since we test assemble() method there's no need of columns mapping because they are used at insertion time
			Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKey2ForeignKeyMapping = Collections.emptyMap();
			Map<Column<RIGHTTABLE, ?>, Column<T, ?>> rightTable2EntryKeyMapping = Collections.emptyMap();
			RecordIdAssembler<String, Long, T> testInstance = new RecordIdAssembler<>(
					joinTable,
					entryKeyIdentifierAssembler,
					sourceIdentifierAssembler,
					primaryKey2ForeignKeyMapping,
					rightTable2EntryKeyMapping
			);
			
			Row row = new Row();
			row.add("id", 42L);
			row.add("entryKey", "toto");
			ColumnedRow rowAliaser = new ColumnedRow(selectable -> {
				// Only leftTableIdColumn and rightTableIdColumn are expected to be read by assemble()
				// Note that they should be in final select query since they are joined on association table
				if (selectable == leftTableIdColumn) {
					return "id";
				}
				if (selectable == rightTableIdColumn) {
					return "entryKey";
				}
				throw new IllegalArgumentException("Column " + selectable + " is not mapped in row");
			});
			
			RecordId<String, Long> actual = testInstance.assemble(row, rowAliaser);
			assertThat(actual).isEqualTo(new RecordId<>(42L, "toto"));
		}
		
		@Test
		<T extends Table<T>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		void getColumnValues_mapEntryKeyIsASimpleProperty_givesRightValues() {
			T joinTable = (T) new Table("joinTable");
			Column<T, Long> joinTableIdColumn = joinTable.addColumn("id", long.class);
			Column<T, String> entryKeyColumn = joinTable.addColumn("entryKey", String.class);
			LEFTTABLE leftTable = (LEFTTABLE) new Table("leftTable");
			Column<LEFTTABLE, Long> leftTableIdColumn = leftTable.addColumn("id", long.class);
			BiFunction<Row, ColumnedRow, String> entryKeyAssembler = (row, columnedRow) -> columnedRow.getValue(entryKeyColumn, row);
			IdentifierAssembler<Long, LEFTTABLE> sourceIdentifierAssembler = new SimpleIdentifierAssembler<>(leftTableIdColumn);
			Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKey2ForeignKeyMapping = (Map) Maps.forHashMap(Column.class, Column.class)
					.add(leftTableIdColumn, joinTableIdColumn);
			RecordIdAssembler<String, Long, T> testInstance = new RecordIdAssembler<>(
					joinTable,
					entryKeyAssembler,
					s -> (Map) Maps.forHashMap(Column.class, String.class)
							.add(entryKeyColumn, s),
					sourceIdentifierAssembler,
					primaryKey2ForeignKeyMapping
			);
			
			
			Map<Column<T, ?>, ?> actual = testInstance.getColumnValues(new RecordId<>(42L, "toto"));
			assertThat(actual).isEqualTo(Maps.forHashMap(Column.class, Object.class)
					.add(joinTableIdColumn, 42L)
					.add(entryKeyColumn, "toto"));
		}
		
		@Test
		<T extends Table<T>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		void getColumnValues_mapEntryKeyIsEntityWithCompositeKey_givesRightValues() {
			T joinTable = (T) new Table("joinTable");
			Column<T, Long> joinTableIdColumn = joinTable.addColumn("id", long.class);
			Column<T, Long> entryKeyProp1Column = joinTable.addColumn("entryKey_prop1", Long.class);
			Column<T, String> entryKeyProp2Column = joinTable.addColumn("entryKey_prop2", String.class);
			LEFTTABLE leftTable = (LEFTTABLE) new Table("leftTable");
			Column<LEFTTABLE, Long> leftTableIdColumn = leftTable.addColumn("id", long.class);
			RIGHTTABLE rightTable = (RIGHTTABLE) new Table("rightTable");
			Column<RIGHTTABLE, Long> rightTableEntryKeyProp1Column = rightTable.addColumn("entryKey_prop1", Long.class);
			Column<RIGHTTABLE, String> rightTableEntryKeyProp2Column = rightTable.addColumn("entryKey_prop2", String.class);
			IdentifierAssembler<EntryKey, RIGHTTABLE> entryKeyIdentifierAssembler = new ComposedIdentifierAssembler<EntryKey, RIGHTTABLE>(rightTable) {
				@Override
				public EntryKey assemble(Function<Column<?, ?>, Object> columnValueProvider) {
					return new EntryKey((Long) columnValueProvider.apply(entryKeyProp1Column), (String) columnValueProvider.apply(entryKeyProp2Column));
				}
				
				@Override
				public Map<Column<RIGHTTABLE, ?>, Object> getColumnValues(EntryKey id) {
					return Maps.forHashMap((Class<Column<RIGHTTABLE, ?>>) (Class) Column.class, Object.class)
							.add(rightTableEntryKeyProp1Column, id.prop1)
							.add(rightTableEntryKeyProp2Column, id.prop2);
				}
			};
			IdentifierAssembler<Long, LEFTTABLE> sourceIdentifierAssembler = new SimpleIdentifierAssembler<>(leftTableIdColumn);
			Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKey2ForeignKeyMapping = (Map) Maps.forHashMap(Column.class, Column.class)
					.add(leftTableIdColumn, joinTableIdColumn);
			Map<Column<RIGHTTABLE, ?>, Column<T, ?>> rightTable2EntryKeyMapping = (Map) Maps.forHashMap(Column.class, Column.class)
					.add(rightTableEntryKeyProp1Column, entryKeyProp1Column)
					.add(rightTableEntryKeyProp2Column, entryKeyProp2Column);
			RecordIdAssembler<EntryKey, Long, T> testInstance = new RecordIdAssembler<>(
					joinTable,
					entryKeyIdentifierAssembler,
					sourceIdentifierAssembler,
					primaryKey2ForeignKeyMapping,
					rightTable2EntryKeyMapping
			);
			
			Map<Column<T, ?>, ?> actual = testInstance.getColumnValues(new RecordId<>(42L, new EntryKey(17, "toto")));
			assertThat(actual).isEqualTo(Maps.forHashMap(Column.class, Object.class)
					.add(joinTableIdColumn, 42L)
					.add(entryKeyProp1Column, 17L)
					.add(entryKeyProp2Column, "toto"));
		}
	}
	
	private static class EntryKey {
		
		private final long prop1;
		private final String prop2;
		
		public EntryKey(long prop1, String prop2) {
			this.prop1 = prop1;
			this.prop2 = prop2;
		}
	}
}