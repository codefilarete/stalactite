package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.dsl.FluentMappings;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

class AggregateMetadataResolverTest {
	
	@Nested
	class OneToOne {
		
		@Test
		void collect_oneEntity() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = FluentMappings
					.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getCapital, entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName));
			
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), Mockito.mock(ConnectionConfiguration.class));
			Entity<Country, Identifier<Long>, ?> actual = testInstance.resolve(mappingBuilder.getConfiguration());
			System.out.println(actual);

//		InheritanceMetadataResolver<E, Integer, ?> testInstance = new InheritanceMetadataResolver<>(new DefaultDialect(), mock(ConnectionConfiguration.class));
//		Entity<E, Integer, ?> entity = testInstance.resolve(entityMappingBuilder.getConfiguration());
//		assertThat(entity.getEntityType()).isEqualTo(E.class);
//		assertThat(entity.getIdAccessor()).isEqualTo(readWriteAccessPoint(E::getPropE));
//		assertThat(entity.getTable().getName()).isEqualTo("E");
//		assertThat(entity.getPropertyMappingHolder().getWritablePropertyToColumn())
//				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
//				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
//				.isEqualTo(Arrays.asSet(
//						new Entity.PropertyMapping<>(readWriteAccessPoint(D::getPropD), entity.getTable().getColumn("propD"), false, null, null, false)
//				));
//		assertThat(entity.getPropertyMappingHolder().getReadonlyPropertyToColumn()).isEmpty();
//		assertThat(entity.getVersioning()).isNull();
		}
	}
	
}
