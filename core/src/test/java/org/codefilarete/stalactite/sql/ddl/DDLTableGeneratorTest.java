package org.codefilarete.stalactite.sql.ddl;

import javax.annotation.Nonnull;
import java.util.Collections;

import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Index;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DDLTableGeneratorTest {
	
	@Test
	public void testGenerateCreateTable() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		Table t = new Table(null, "Toto");
		
		t.addColumn("A", String.class);
		String generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A type)");
		
		t.addColumn("B", String.class);
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A type, B type)");
		
		Column<Table, String> primaryKey = t.addColumn("C", String.class);
		primaryKey.setPrimaryKey(true);
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A type, B type, C type, primary key (C))");
		
		t.addColumn("D", Integer.TYPE);	// test isNullable
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A type, B type, C type, D type not null, primary key (C))");
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == primaryKey) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		t.addColumn("D", Integer.TYPE);	// test isNullable
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A type, B type, 'key' type, D type not null, primary key ('key'))");
	}
	
	@Test
	public void testGenerateDropTable() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		
		String generateDropTable = testInstance.generateDropTable(toto);
		assertThat(generateDropTable).isEqualTo("drop table Toto");
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Table table) {
				if (table == toto) {
					return "'user'";
				}
				return super.getSimpleName(toto);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		
		generateDropTable = testInstance.generateDropTable(toto);
		assertThat(generateDropTable).isEqualTo("drop table 'user'");
	}
	
	@Test
	public void testGenerateDropTableIfExists() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		
		String generateDropTable = testInstance.generateDropTableIfExists(toto);
		assertThat(generateDropTable).isEqualTo("drop table if exists Toto");
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Table table) {
				if (table == toto) {
					return "'user'";
				}
				return super.getSimpleName(toto);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		
		generateDropTable = testInstance.generateDropTableIfExists(toto);
		assertThat(generateDropTable).isEqualTo("drop table if exists 'user'");
	}
	
	@Test
	public void testGenerateAddColumn() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		Table t = new Table(null, "Toto");
		
		Column<Table, String> newColumn = t.addColumn("A", String.class);
		String generateAddColumn = testInstance.generateAddColumn(newColumn);
		assertThat(generateAddColumn).isEqualTo("alter table Toto add column A type");
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == newColumn) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateAddColumn = testInstance.generateAddColumn(newColumn);
		assertThat(generateAddColumn).isEqualTo("alter table Toto add column 'key' type");
	}
	
	@Test
	public void testGenerateDropColumn() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		Table t = new Table(null, "Toto");
		
		Column<Table, String> newColumn = t.addColumn("A", String.class);
		String generateDropColumn = testInstance.generateDropColumn(newColumn);
		assertThat(generateDropColumn).isEqualTo("alter table Toto drop column A");
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == newColumn) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateDropColumn = testInstance.generateDropColumn(newColumn);
		assertThat(generateDropColumn).isEqualTo("alter table Toto drop column 'key'");
	}
	
	@Test
	public void testGenerateCreateIndex() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table t = new Table(null, "Toto");
		Column colA = t.addColumn("A", String.class);
		Column colB = t.addColumn("B", String.class);
		
		Index idx1 = t.addIndex("Idx1", colA);
		String generatedCreateIndex = testInstance.generateCreateIndex(idx1);
		assertThat(generatedCreateIndex).isEqualTo("create index Idx1 on Toto(A)");
		
		Index idx2 = t.addIndex("Idx2", colA, colB);
		generatedCreateIndex = testInstance.generateCreateIndex(idx2);
		assertThat(generatedCreateIndex).isEqualTo("create index Idx2 on Toto(A, B)");

		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		t.addColumn("D", Integer.TYPE);	// test isNullable
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		generatedCreateIndex = testInstance.generateCreateIndex(idx2);
		assertThat(generatedCreateIndex).isEqualTo("create index Idx2 on Toto('key', B)");
	}
	
	@Test
	public void testGenerateDropIndex() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table t = new Table(null, "Toto");
		Column<Table, String> colA = t.addColumn("A", String.class);
		
		Index idx = t.addIndex("idx1", colA);
		
		String generateDropIndex = testInstance.generateDropIndex(idx);
		assertThat(generateDropIndex).isEqualTo("drop index idx1");
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateDropIndex = testInstance.generateDropIndex(idx);
		assertThat(generateDropIndex).isEqualTo("drop index idx1");
	}
	
	@Test
	public void testGenerateCreateForeignKey() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		Column colB = toto.addColumn("B", String.class);
		
		Table titi = new Table(null, "Titi");
		Column colA2 = titi.addColumn("A", String.class);
		Column colB2 = titi.addColumn("B", String.class);
		
		ForeignKey foreignKey = toto.addForeignKey("FK1", colA, colA2);
		String generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertThat(generatedCreateIndex).isEqualTo("alter table Toto add constraint FK1 foreign key(A) references Titi(A)");
		
		foreignKey = toto.addForeignKey("FK2", Arrays.asList(colA, colB), Arrays.asList(colA2, colB2));
		generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertThat(generatedCreateIndex).isEqualTo("alter table Toto add constraint FK2 foreign key(A, B) references Titi(A, B)");

		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA || column == colA2) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		generatedCreateIndex = testInstance.generateCreateForeignKey(foreignKey);
		assertThat(generatedCreateIndex).isEqualTo("alter table Toto add constraint FK2 foreign key('key', B) references Titi('key', B)");
	}
	
	@Test
	public void testGenerateDropForeignKey() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null);
		
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		
		Table titi = new Table(null, "Titi");
		Column colA2 = titi.addColumn("A", String.class);
		
		ForeignKey foreignKey = toto.addForeignKey("FK1", colA, colA2);
		
		String generateDropForeignKey = testInstance.generateDropForeignKey(foreignKey);
		assertThat(generateDropForeignKey).isEqualTo("alter table Toto drop constraint FK1");
		
		// test with a non default DMLNameProvider
		DMLNameProvider dmlNameProvider = new DMLNameProvider(Collections.emptyMap()) {
			@Override
			public String getSimpleName(@Nonnull Column column) {
				if (column == colA) {
					return "'key'";
				}
				return super.getSimpleName(column);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider) {
			@Override
			protected String getSqlType(Column column) {
				return "type";
			}
		};
		
		generateDropForeignKey = testInstance.generateDropForeignKey(foreignKey);
		assertThat(generateDropForeignKey).isEqualTo("alter table Toto drop constraint FK1");
	}
}