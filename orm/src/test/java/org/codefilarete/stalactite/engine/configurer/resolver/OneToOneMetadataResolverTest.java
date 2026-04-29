package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.device.Address;
import org.codefilarete.stalactite.engine.model.device.Device;
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
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
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
	
	@Nested
	class Inheritance {
		
		@Test
		void ownedBySource() {
			Table countryTable = new Table("country");
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryMappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.onTable(countryTable)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
							.mapKey(Person::getId, ALREADY_ASSIGNED)
							.map(Person::getName));
			
			Table republicTable = new Table("republic");
			FluentEntityMappingBuilder<Republic, Identifier<Long>> mappingBuilder = entityBuilder(Republic.class, Identifier.LONG_TYPE)
					.onTable(republicTable)
					.mapSuperClass(countryMappingBuilder)
					.joiningTables();
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Republic, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).isEmpty();
			Entity<? super Republic, Identifier<Long>, ?> republicEntity = countryEntity.getParent().getAncestor();
			assertThat(republicEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(republicEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table actualCountryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(actualCountryTable.getColumn("presidentId"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table actualPersonTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(actualPersonTable.getColumn("id"));
		}
	}
	
	@Nested
	class Embedded {
		
		@Test
		void ownedBySource() {
			FluentEmbeddableMappingBuilder<Address> addressMappingBuilder = embeddableBuilder(Address.class)
					.map(Address::getStreet)
					.mapOneToOne(Address::getCity, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName));
			
			FluentEntityMappingBuilder<Device, Identifier<Long>> mappingBuilder = entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, ALREADY_ASSIGNED)
					.map(Device::getName)
					.embed(Device::setLocation, addressMappingBuilder);
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Device, Identifier<Long>, ?> deviceEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(deviceEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(deviceEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table deviceTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(deviceTable.getColumn("location_cityId"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table addressTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(addressTable.getColumn("id"));
			
		}
		
		@Test
		void ownedBySource_columnName() {
			FluentEmbeddableMappingBuilder<Address> addressMappingBuilder = embeddableBuilder(Address.class)
					.map(Address::getStreet)
					.mapOneToOne(Address::getCity, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName))
					.columnName("locationCityId");
			
			FluentEntityMappingBuilder<Device, Identifier<Long>> mappingBuilder = entityBuilder(Device.class, Identifier.LONG_TYPE)
					.mapKey(Device::getId, ALREADY_ASSIGNED)
					.map(Device::getName)
					.embed(Device::setLocation, addressMappingBuilder);
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Device, Identifier<Long>, ?> deviceEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(deviceEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(deviceEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table deviceTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(deviceTable.getColumn("locationCityId"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table addressTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(addressTable.getColumn("id"));
		}
		
		@Test
		void inherited() {
			Table countryTable = new Table("country");
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryMappingBuilder = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.onTable(countryTable)
					.map(Country::getName)
					.embed(Country::getPresident, embeddableBuilder(Person.class)
							.map(Person::getName)
							.mapOneToOne(Person::getVehicle, entityBuilder(Car.class, Identifier.LONG_TYPE)
									.mapKey(Car::getId, ALREADY_ASSIGNED)
									.map(Car::getModel)));
			
			Table republicTable = new Table("republic");
			FluentEntityMappingBuilder<Republic, Identifier<Long>> mappingBuilder = entityBuilder(Republic.class, Identifier.LONG_TYPE)
					.onTable(republicTable)
					.mapSuperClass(countryMappingBuilder)
					.joiningTables();
			
			// because building the objects consumed by OneToOneMetadataResolver is complex, we use AggregateMetadataResolver, which do it, as a test instance 
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<Republic, Identifier<Long>, ?> countryEntity = testInstance.resolve(mappingBuilder.getConfiguration());
			
			assertThat(countryEntity.getRelations()).isEmpty();
			Entity<? super Republic, Identifier<Long>, ?> republicEntity = countryEntity.getParent().getAncestor();
			assertThat(republicEntity.getRelations()).hasSize(1);
			MappingJoin<?, ?, ?> relation = first(republicEntity.getRelations());
			assertThat(relation).isInstanceOf(ResolvedOneToOneRelation.class);
			
			Key<?, ?> leftKey = relation.getJoin().getLeftKey();
			Table actualCountryTable = (Table) leftKey.getTable();
			KeepOrderSet<Column<?, ?>> leftKeyColumns = (KeepOrderSet<Column<?, ?>>) leftKey.getColumns();
			assertThat(leftKeyColumns).containsExactly(actualCountryTable.getColumn("president_vehicleId"));
			
			Key<?, ?> rightKey = relation.getJoin().getRightKey();
			Table actualPersonTable = (Table) rightKey.getTable();
			KeepOrderSet<Column<?, ?>> rightKeyColumns = (KeepOrderSet<Column<?, ?>>) rightKey.getColumns();
			assertThat(rightKeyColumns).containsExactly(actualPersonTable.getColumn("id"));
		}
	}
}
