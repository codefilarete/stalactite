package org.codefilarete.stalactite.engine.configurer;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.MapBasedColumnedRow;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.Accessors.*;

class AssociationRecordMappingTest {
	
	@Nested
	class SingleColumnKey {
		@Test
		<ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, Integer, Integer>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		void write_read() {
			LEFTTABLE leftTable = (LEFTTABLE) new Table<>("leftTable");
			Column<LEFTTABLE, Integer> leftTableIdColumn = leftTable.addColumn("id", int.class).primaryKey();
			PrimaryKey<LEFTTABLE, Integer> leftTablePrimaryKey = leftTable.getPrimaryKey();
			RIGHTTABLE rightTable = (RIGHTTABLE) new Table<>("rightTable");
			Column<RIGHTTABLE, Integer> rightTableIdColumn = rightTable.addColumn("id", int.class).primaryKey();
			PrimaryKey<RIGHTTABLE, Integer> rightTablePrimaryKey = rightTable.getPrimaryKey();
			
			AssociationTableNamingStrategy.ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> columnNames = AssociationTableNamingStrategy.DEFAULT.giveColumnNames(
					new AccessorDefinition(Country.class, "cities", City.class),
					leftTablePrimaryKey,
					rightTablePrimaryKey);
			AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, Integer, Integer> associationTable = new AssociationTable<>(null,
					"associationTable",
					leftTablePrimaryKey,
					rightTablePrimaryKey,
					columnNames,
					ForeignKeyNamingStrategy.DEFAULT,
					true,
					true);

			AssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, Integer, Integer> testInstance = new AssociationRecordMapping<>(
					(ASSOCIATIONTABLE) associationTable,
					new SingleIdentifierAssembler<>(leftTableIdColumn),
					new SingleIdentifierAssembler<>(rightTableIdColumn));

			Map<Column<ASSOCIATIONTABLE, ?>, ?> insertValues = testInstance.getInsertValues(new AssociationRecord(42, 666));
			assertThat(insertValues.get(associationTable.getLeftIdentifierColumnMapping().get(leftTableIdColumn))).isEqualTo(42);
			assertThat(insertValues.get(associationTable.getRightIdentifierColumnMapping().get(rightTableIdColumn))).isEqualTo(666);
			
			MapBasedColumnedRow row = new MapBasedColumnedRow();
			// values are taken on the association table, thus we have to fill the row with the appropriate columns
			row.add(associationTable.getColumn("leftTable_id"), 42);
			row.add(associationTable.getColumn("cities_id"), 666);
			AssociationRecord readAssociationRecord = testInstance.getRowTransformer().transform(row);
			assertThat(readAssociationRecord.getLeft()).isEqualTo(42);
			assertThat(readAssociationRecord.getRight()).isEqualTo(666);
		}
	}
	
	@Nested
	class CompositeKey {
		@Test
		<ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, MavenProject, MavenProject>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		void write_read() {
			LEFTTABLE leftTable = (LEFTTABLE) new Table<>("leftTable");
			Column<LEFTTABLE, String> leftTableGroupIdColumn = leftTable.addColumn("groupId", String.class).primaryKey();
			Column<LEFTTABLE, String> leftTableArtefactIdColumn = leftTable.addColumn("artefactId", String.class).primaryKey();
			Column<LEFTTABLE, String> leftTableVersionColumn = leftTable.addColumn("version", String.class).primaryKey();
			PrimaryKey<LEFTTABLE, MavenProject> leftTablePrimaryKey = leftTable.getPrimaryKey();
			RIGHTTABLE rightTable = (RIGHTTABLE) new Table<>("rightTable");
			Column<RIGHTTABLE, String> rightTableGroupIdColumn = rightTable.addColumn("groupId", String.class).primaryKey();
			Column<RIGHTTABLE, String> rightTableArtefactIdColumn = rightTable.addColumn("artefactId", String.class).primaryKey();
			Column<RIGHTTABLE, String> rightTableVersionColumn = rightTable.addColumn("version", String.class).primaryKey();
			PrimaryKey<RIGHTTABLE, MavenProject> rightTablePrimaryKey = rightTable.getPrimaryKey();
			
			AssociationTableNamingStrategy.ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> columnNames = AssociationTableNamingStrategy.DEFAULT.giveColumnNames(
					new AccessorDefinition(MavenProject.class, "dependencies", MavenProject.class),
					leftTablePrimaryKey,
					rightTablePrimaryKey);
			ASSOCIATIONTABLE associationTable = (ASSOCIATIONTABLE) new AssociationTable<>(null,
					"associationTable",
					leftTablePrimaryKey,
					rightTablePrimaryKey,
					columnNames,
					ForeignKeyNamingStrategy.DEFAULT,
					true,
					true);

			Map<PropertyAccessor, Column> leftMapping = Maps.forHashMap(PropertyAccessor.class, Column.class)
					.add(new PropertyAccessor<>(accessorByMethodReference(MavenProject::getGroupId), mutatorByMethodReference(MavenProject::setGroupId)), leftTableGroupIdColumn)
					.add(new PropertyAccessor<>(accessorByMethodReference(MavenProject::getArtifactId), mutatorByMethodReference(MavenProject::setArtifactId)), leftTableArtefactIdColumn)
					.add(new PropertyAccessor<>(accessorByMethodReference(MavenProject::getVersion), mutatorByMethodReference(MavenProject::setVersion)), leftTableVersionColumn);

			Map<PropertyAccessor, Column> rightMapping = Maps.forHashMap(PropertyAccessor.class, Column.class)
					.add(new PropertyAccessor<>(accessorByMethodReference(MavenProject::getGroupId), mutatorByMethodReference(MavenProject::setGroupId)), rightTableGroupIdColumn)
					.add(new PropertyAccessor<>(accessorByMethodReference(MavenProject::getArtifactId), mutatorByMethodReference(MavenProject::setArtifactId)), rightTableArtefactIdColumn)
					.add(new PropertyAccessor<>(accessorByMethodReference(MavenProject::getVersion), mutatorByMethodReference(MavenProject::setVersion)), rightTableVersionColumn);

			AssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, MavenProject, MavenProject> testInstance = new AssociationRecordMapping<>(
					associationTable,
					new DefaultComposedIdentifierAssembler<>(leftTable,
							MavenProject.class,
							(Map<? extends ReversibleAccessor<MavenProject, ?>, ? extends Column<LEFTTABLE, ?>>) (Map) leftMapping),
					new DefaultComposedIdentifierAssembler<>(rightTable,
							MavenProject.class,
							(Map<? extends ReversibleAccessor<MavenProject, ?>, ? extends Column<RIGHTTABLE, ?>>) (Map) rightMapping));

			Map<Column<ASSOCIATIONTABLE, ?>, ?> insertValues = testInstance.getInsertValues(new AssociationRecord(
					new MavenProject("a", "b", "c"),
					new MavenProject("x", "y", "z")));
			assertThat(insertValues.get(associationTable.getLeftIdentifierColumnMapping().get(leftTableGroupIdColumn))).isEqualTo("a");
			assertThat(insertValues.get(associationTable.getLeftIdentifierColumnMapping().get(leftTableArtefactIdColumn))).isEqualTo("b");
			assertThat(insertValues.get(associationTable.getLeftIdentifierColumnMapping().get(leftTableVersionColumn))).isEqualTo("c");
			assertThat(insertValues.get(associationTable.getRightIdentifierColumnMapping().get(rightTableGroupIdColumn))).isEqualTo("x");
			assertThat(insertValues.get(associationTable.getRightIdentifierColumnMapping().get(rightTableArtefactIdColumn))).isEqualTo("y");
			assertThat(insertValues.get(associationTable.getRightIdentifierColumnMapping().get(rightTableVersionColumn))).isEqualTo("z");
			
			MapBasedColumnedRow row = new MapBasedColumnedRow();
			// values are taken on the association table, thus we have to fill the row with the appropriate columns
			row.add(associationTable.getColumn("leftTable_groupId"), "a");
			row.add(associationTable.getColumn("leftTable_artefactId"), "b");
			row.add(associationTable.getColumn("leftTable_version"), "c");
			row.add(associationTable.getColumn("dependencies_groupId"), "x");
			row.add(associationTable.getColumn("dependencies_artefactId"), "y");
			row.add(associationTable.getColumn("dependencies_version"), "z");
			AssociationRecord readAssociationRecord = testInstance.getRowTransformer().transform(row);
			assertThat(readAssociationRecord.getLeft()).isEqualTo(new MavenProject("a", "b", "c"));
			assertThat(readAssociationRecord.getRight()).isEqualTo(new MavenProject("x", "y", "z"));
		}
	}
	
	static class MavenProject {
		
		private String groupId;
		private String artifactId;
		private String version;
		
		private Set<MavenProject> dependencies;

		public MavenProject() {
		}

		public MavenProject(String groupId, String artifactId, String version) {
			this.groupId = groupId;
			this.artifactId = artifactId;
			this.version = version;
		}

		public String getGroupId() {
			return groupId;
		}

		public void setGroupId(String groupId) {
			this.groupId = groupId;
		}

		public String getArtifactId() {
			return artifactId;
		}

		public void setArtifactId(String artifactId) {
			this.artifactId = artifactId;
		}

		public String getVersion() {
			return version;
		}

		public void setVersion(String version) {
			this.version = version;
		}

		public Set<MavenProject> getDependencies() {
			return dependencies;
		}

		public void setDependencies(Set<MavenProject> dependencies) {
			this.dependencies = dependencies;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			MavenProject that = (MavenProject) o;
			return Objects.equals(groupId, that.groupId) && Objects.equals(artifactId, that.artifactId) && Objects.equals(version, that.version);
		}

		@Override
		public int hashCode() {
			return Objects.hash(groupId, artifactId, version);
		}

		@Override
		public String toString() {
			return "MavenProject{" +
					"version='" + version + '\'' +
					", artifactId='" + artifactId + '\'' +
					", groupId='" + groupId + '\'' +
					'}';
		}
	}
}
