package org.codefilarete.stalactite.sql.ddl.structure;

import org.codefilarete.stalactite.sql.ddl.Length;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.sql.ddl.Size.length;

/**
 * @author Guillaume Mary
 */
class TableTest {
	
	@Test
	void addColumn_alreadyExists_returnsExistingOne() {
		Table testInstance = new Table("toto");
		// empty table shouldn't throw any exception nor found anything
		Column xxColumn = testInstance.addColumn("xx", String.class);
		
		// same column with same type doesn't has any consequence
		Column newColumn = testInstance.addColumn("xx", String.class);
		assertThat(newColumn).isSameAs(xxColumn);
	}
	
	@Test
	void addColumn_alreadyExistsWithDifferentType_throwsException() {
		Table testInstance = new Table("toto");
		// empty table shouldn't throw any exception nor found anything
		testInstance.addColumn("xx", String.class);
		
		// same column with other type throws exception
		assertThatThrownBy(() -> testInstance.addColumn("xx", Integer.class))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Trying to add column 'xx' to 'toto' but it already exists with a different type : j.l.String vs j.l.Integer, nullable null vs null");
	}
	
	@Test
	void addColumn_alreadyExistsWithDifferentSize() {
		Table testInstance = new Table("toto");
		// empty table shouldn't throw any exception nor found anything
		Column columnWithoutSize = testInstance.addColumn("columnWithoutSize", String.class);
		Column columnWithSize = testInstance.addColumn("columnWithSize", String.class, length(36));
		
		assertThat(testInstance.addColumn("columnWithoutSize", String.class)).isSameAs(columnWithoutSize);
		assertThat(testInstance.addColumn("columnWithoutSize", String.class, length(42)))
				.isSameAs(columnWithoutSize)
				.extracting(c -> ((Length) c.getSize()).getValue()).isEqualTo(42);
		assertThat(testInstance.addColumn("columnWithSize", String.class)).isSameAs(columnWithSize);
		
		// same column with other type throws exception
		assertThatThrownBy(() -> testInstance.addColumn("columnWithSize", String.class, length(12)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Trying to add column 'columnWithSize' to 'toto' but it already exists with a different type : j.l.String(36) vs j.l.String(12), nullable null vs null");
	}
	
	@Test
	void addForeignKey_alreadyExists_returnsExistingOne() {
		Table testInstance = new Table("toto");
		Column xColumn = testInstance.addColumn("x", String.class);
		Table referencedInstance = new Table("tata");
		Column yColumn = referencedInstance.addColumn("y", String.class);
		// empty table shouldn't throw any exception nor found anything
		ForeignKey fk = testInstance.addForeignKey("dummy FK name", xColumn, yColumn);
		
		// same column with same type doesn't has any consequence
		ForeignKey newFK = testInstance.addForeignKey("dummy FK name", xColumn, yColumn);
		assertThat(newFK).isSameAs(fk);
	}
	
	@Test
	void addForeignKey_alreadyExistsWithDifferentType_throwsException() {
		Table testInstance = new Table("toto");
		Column xColumn = testInstance.addColumn("x", String.class);
		Column xxColumn = testInstance.addColumn("xx", String.class);
		Table referencedInstance = new Table("tata");
		Column yColumn = referencedInstance.addColumn("y", String.class);
		// empty table shouldn't throw any exception nor found anything
		testInstance.addForeignKey("dummy FK name", xColumn, yColumn);
		
		// same column with other type throws exception
		assertThatThrownBy(() -> testInstance.addForeignKey("dummy FK name", xxColumn, yColumn))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Trying to add a foreign key with same name than another with different columns : dummy FK name "
						+ "toto.x -> tata.y vs toto.xx -> tata.y");
	}
	
	@Test
	void findColumn() {
		Table testInstance = new Table("toto");
		// empty columns should throw any exception nor found anything
		assertThat(testInstance.findColumn("xx")).isNull();
		
		// basic case
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertThat(testInstance.findColumn("name")).isSameAs(nameColumn);
		// xx still doesn't exist
		assertThat(testInstance.findColumn("xx")).isNull();
	}
	
	@Test
	void getAbsoluteName() {
		Table testInstance = new Table("toto");
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertThat(nameColumn.getAbsoluteName()).isEqualTo("toto.name");
	}
	
	@Test
	void getAlias() {
		Table testInstance = new Table("toto");
		Column nameColumn = testInstance.addColumn("name", String.class);
		assertThat(nameColumn.getAlias()).isEqualTo("toto_name");
	}
	
	@Test
	void getPrimaryKey() {
		Table testInstance = new Table("toto");
		assertThat(testInstance.getPrimaryKey()).isNull();
		
		testInstance.addColumn("dummyColumn", String.class);
		assertThat(testInstance.getPrimaryKey()).isNull();
		
		Column id = testInstance.addColumn("id", long.class);
		id.primaryKey();
		Column subId = testInstance.addColumn("subId", long.class);
		subId.primaryKey();
		assertThat(testInstance.getPrimaryKey()).isNotNull();
		assertThat(testInstance.getPrimaryKey().getColumns()).containsExactly(id, subId);
	}
	
	@Test
	void getColumnsPrimaryKey() {
		Table testInstance = new Table("toto");
		assertThat(testInstance.getPrimaryKey()).isNull();
		
		Column dummyColumn = testInstance.addColumn("dummyColumn", String.class);
		Column id = testInstance.addColumn("id", long.class).primaryKey();
		assertThat(testInstance.getPrimaryKey().getColumns()).containsExactly(id);
		assertThat(testInstance.getColumnsNoPrimaryKey()).isEqualTo(Arrays.asHashSet(dummyColumn));
	}
	
	@Test
	void addForeignKey() {
		Table testInstance1 = new Table("toto");
		Column tataId = testInstance1.addColumn("tataId", Integer.class);
		Table testInstance2 = new Table("tata");
		Column id = testInstance2.addColumn("id", Integer.class);
		
		ForeignKey createdFK = testInstance1.addForeignKey("XX", tataId, id);
		
		assertThat(createdFK.getName()).isEqualTo("XX");
		assertThat(createdFK.getColumns()).isEqualTo(new KeepOrderSet<>(tataId));
		assertThat(createdFK.getTable()).isEqualTo(testInstance1);
		assertThat(createdFK.getTargetColumns()).isEqualTo(new KeepOrderSet<>(id));
		assertThat(createdFK.getTargetTable()).isEqualTo(testInstance2);
	}
	
	@Test
	void addForeignKey_withNamingFunction() {
		Table<?> testInstance1 = new Table("toto");
		Column tataId = testInstance1.addColumn("tataId", Integer.class);
		Table testInstance2 = new Table("tata");
		Column id = testInstance2.addColumn("id", Integer.class);
		
		ForeignKey createdFK = testInstance1.addForeignKey((c1, c2) -> c1.getName() + "_" + c2.getName(), tataId, id);
		
		assertThat(createdFK.getName()).isEqualTo("tataId_id");
		assertThat(createdFK.getColumns()).isEqualTo(new KeepOrderSet<>(tataId));
		assertThat(createdFK.getTable()).isEqualTo(testInstance1);
		assertThat(createdFK.getTargetColumns()).isEqualTo(new KeepOrderSet<>(id));
		assertThat(createdFK.getTargetTable()).isEqualTo(testInstance2);
	}
}