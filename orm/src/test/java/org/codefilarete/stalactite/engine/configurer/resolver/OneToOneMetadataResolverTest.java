package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.mockito.Mockito.mock;

class OneToOneMetadataResolverTest {
	
	@Nested
	class OwnedBySource {
		
		@Test
		void ownedBySource() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
							.mapKey(Person::getId, ALREADY_ASSIGNED)
							.map(Person::getName));
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Country, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(countryEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table countryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(countryTable.getColumn("presidentId"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table personTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(personTable.getColumn("id"));
		}
		
		@Test
		void ownedBySource_columnName() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
							.mapKey(Person::getId, ALREADY_ASSIGNED)
							.map(Person::getName))
					.columnName("president");
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Country, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(countryEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table countryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(countryTable.getColumn("president"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table personTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(personTable.getColumn("id"));
		}
	}
	
	@Nested
	class OwnedByTarget {
		
		@Test
		void ownedByTarget_byColumnName() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
							.mapKey(Person::getId, ALREADY_ASSIGNED)
							.map(Person::getName))
					.reverseJoinColumn("presidentOf");
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Country, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(countryEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table countryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(countryTable.getColumn("id"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table personTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(personTable.getColumn("presidentOf"));
		}
		
		@Test
		void ownedByTarget_byColumn() {
			Table personTable = new Table("city");
			Column<Table, Identifier<Long>> presidentColumn = personTable.addColumn("presidentOf", Identifier.LONG_TYPE);
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
							.onTable(personTable)
							.mapKey(Person::getId, ALREADY_ASSIGNED)
							.map(Person::getName))
					.reverseJoinColumn(presidentColumn);
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Country, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(countryEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table countryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(countryTable.getColumn("id"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(presidentColumn);
		}
		
		@Test
		void ownedByTarget_byReverseGetter() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
							.mapKey(Person::getId, ALREADY_ASSIGNED)
							.map(Person::getName))
					.mappedBy(Person::getCountry);
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Country, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(countryEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table countryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(countryTable.getColumn("id"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table personTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(personTable.getColumn("presidentId"));
		}
		
		@Test
		void ownedByTarget_byReverseSetter() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
							.mapKey(Person::getId, ALREADY_ASSIGNED)
							.map(Person::getName))
					.mappedBy(Person::setCountry);
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Country, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(countryEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table countryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(countryTable.getColumn("id"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table personTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(personTable.getColumn("presidentId"));
		}
	}
}
