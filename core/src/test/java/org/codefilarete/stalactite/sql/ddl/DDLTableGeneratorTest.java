package org.codefilarete.stalactite.sql.ddl;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Index;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.stalactite.sql.ddl.Size.fixedPoint;
import static org.codefilarete.stalactite.sql.ddl.Size.length;

public class DDLTableGeneratorTest {
	
	@Test
	void generateCreateTable() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new) {
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
		
		t.addColumn("D", Integer.TYPE, null, false);	// test isNullable
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A type, B type, C type, D type not null, primary key (C))");
		
		// test with a non default DMLNameProvider
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getSimpleName(Selectable<?> column) {
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
	void columnSizeIsTakenIntoAccount() {
		DefaultTypeMapping typeMapping = new DefaultTypeMapping();
		DDLTableGenerator testInstance = new DDLTableGenerator(new SqlTypeRegistry(typeMapping), DMLNameProvider::new);

		Table t = new Table(null, "Toto");

		t.addColumn("A", Path.class, length(100));
		String generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A varchar(100))");

		t.addColumn("B", BigDecimal.class, fixedPoint(9, 4));
		generatedCreateTable = testInstance.generateCreateTable(t);
		assertThat(generatedCreateTable).isEqualTo("create table Toto(A varchar(100), B decimal(9, 4))");
	}
	
	@Test
	void generateDropTable() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new);
		
		Table toto = new Table(null, "Toto");
		
		String generateDropTable = testInstance.generateDropTable(toto);
		assertThat(generateDropTable).isEqualTo("drop table Toto");
		
		// test with a non default DMLNameProvider
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getName(Fromable table) {
				if (table == toto) {
					return "'user'";
				}
				return super.getName(toto);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		
		generateDropTable = testInstance.generateDropTable(toto);
		assertThat(generateDropTable).isEqualTo("drop table 'user'");
	}
	
	@Test
	void generateDropTableIfExists() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new);
		
		Table toto = new Table(null, "Toto");
		
		String generateDropTable = testInstance.generateDropTableIfExists(toto);
		assertThat(generateDropTable).isEqualTo("drop table if exists Toto");
		
		// test with a non default DMLNameProvider
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getName(Fromable table) {
				if (table == toto) {
					return "'user'";
				}
				return super.getName(toto);
			}
		};
		testInstance = new DDLTableGenerator(null, dmlNameProvider);
		
		generateDropTable = testInstance.generateDropTableIfExists(toto);
		assertThat(generateDropTable).isEqualTo("drop table if exists 'user'");
	}
	
	@Test
	void generateAddColumn() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new) {
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
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getSimpleName(Selectable<?> column) {
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
	void generateDropColumn() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new) {
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
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getSimpleName(Selectable<?> column) {
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
	void generateCreateIndex() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new);
		
		Table t = new Table<>(null, "Toto");
		Column<?, String> colA = t.addColumn("A", String.class);
		Column<?, String> colB = t.addColumn("B", String.class);
		
		Index<?> idx1 = t.addIndex("Idx1", colA);
		String generatedCreateIndex = testInstance.generateCreateIndex(idx1);
		assertThat(generatedCreateIndex).isEqualTo("create index Idx1 on Toto(A)");
		
		Index<?> idx2 = t.addIndex("Idx2", colA, colB);
		generatedCreateIndex = testInstance.generateCreateIndex(idx2);
		assertThat(generatedCreateIndex).isEqualTo("create index Idx2 on Toto(A, B)");

		// test with a non default DMLNameProvider
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getSimpleName(Selectable<?> column) {
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
	void generateDropIndex() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new);
		
		Table t = new Table(null, "Toto");
		Column<?, String> colA = t.addColumn("A", String.class);
		
		Index<?> idx = t.addIndex("idx1", colA);
		
		String generateDropIndex = testInstance.generateDropIndex(idx);
		assertThat(generateDropIndex).isEqualTo("drop index idx1");
		
		// test with a non default DMLNameProvider
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getSimpleName(Selectable<?> column) {
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
	void generateCreateForeignKey() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new);
		
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
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getSimpleName(Selectable<?> column) {
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
	void generateDropForeignKey() {
		DDLTableGenerator testInstance = new DDLTableGenerator(null, DMLNameProvider::new);
		
		Table toto = new Table(null, "Toto");
		Column colA = toto.addColumn("A", String.class);
		
		Table titi = new Table(null, "Titi");
		Column colA2 = titi.addColumn("A", String.class);
		
		ForeignKey foreignKey = toto.addForeignKey("FK1", colA, colA2);
		
		String generateDropForeignKey = testInstance.generateDropForeignKey(foreignKey);
		assertThat(generateDropForeignKey).isEqualTo("alter table Toto drop constraint FK1");
		
		// test with a non default DMLNameProvider
		DMLNameProviderFactory dmlNameProvider = tableAliaser -> new DMLNameProvider(tableAliaser) {
			@Override
			public String getSimpleName(Selectable<?> column) {
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
	
	/**
	 * Abstract class that creates some tables, columns, indexes and foreign keys and expose a method to deploy them
	 * on a {@link Connection} thanks to a {@link DDLTableGenerator}.
	 * This default behavior is expected to be implemented by supported-database-adapter modules in their own test
	 * to check that default {@link DDLTableGenerator} generates a syntactically correct SQL for them, or to test their
	 * dedicated implementation.
	 * 
	 * @author Guillaume Mary
	 */
	public abstract static class IntegrationTest {
		
		protected final Table table1;
		protected final Table table2;
		
		public IntegrationTest() {
			table1 = new Table<>("dummyTable1");
			table2 = new Table<>("dummyTable2");
			defineSchema();
		}
		
		protected void defineSchema() {
			// testing primary key and auto-increment
			table1.addColumn("id", int.class).primaryKey().autoGenerated();
			Column<?, String> nameColumn = table1.addColumn("name", String.class);
			// testing index creation
			table1.addIndex("dummyIDX_1", nameColumn);
			// testing unique constraint creation
			table1.addUniqueConstraint("dummy_UK", nameColumn);
			
			Column nameColumn2 = table2.addColumn("name", String.class);
			// testing foreign key constraint creation
			table2.addForeignKey("dummyTable2_FK", nameColumn2, nameColumn);
		}
		
		public void assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(DDLTableGenerator testInstance, Connection connection) {
			DDLDeployer ddlDeployer = new DDLDeployer(testInstance, new DDLSequenceGenerator(testInstance.dmlNameProvider), new SimpleConnectionProvider(connection));
			ddlDeployer.getDdlGenerator().addTables(table1, table2);
			assertThatCode(ddlDeployer::deployDDL).doesNotThrowAnyException();
		}
	}
}
