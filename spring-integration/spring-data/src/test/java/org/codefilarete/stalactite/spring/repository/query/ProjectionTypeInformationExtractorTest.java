package org.codefilarete.stalactite.spring.repository.query;

import javax.sql.DataSource;
import java.util.IdentityHashMap;
import java.util.List;

import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Language;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.EntityProjectionIntrospector;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.util.ClassTypeInformation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.in;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.spring.repository.StalactiteRepositoryFactoryBean.asInternalPersister;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProjectionTypeInformationExtractorTest {

	private static AdvancedEntityPersister<Republic, Identifier<Long>> entityPersister;

	@BeforeAll
	static void initPersister() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");

		dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		dialect.getSqlTypeRegistry().put(Color.class, "int");

		PersistenceContext persistenceContext = new PersistenceContext(mock(DataSource.class), dialect);

		EntityPersister<Republic, Identifier<Long>> persister = entityBuilder(Republic.class, LONG_TYPE)
				.mapKey(Republic::getId, ColumnOptions.IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
				.map(Republic::getName)
				.map(Republic::getDescription)
				.map(Republic::isEuMember)
				.embed(Republic::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.mapOneToOne(Republic::getPresident, entityBuilder(Person.class, LONG_TYPE)
						.mapKey(Person::getId, ColumnOptions.IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
						.map(Person::getName)
						.mapCollection(Person::getNicknames, String.class)
						.mapMap(Person::getPhoneNumbers, String.class, String.class)
						.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ColumnOptions.IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
								.map(Vehicle::getColor)))
				.mapOneToMany(Republic::getStates, entityBuilder(State.class, LONG_TYPE)
						.mapKey(State::getId, ColumnOptions.IdentifierPolicy.<State, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
						.map(State::getName))
				.mapManyToMany(Republic::getLanguages, entityBuilder(Language.class, LONG_TYPE)
						.mapKey(Language::getId, ColumnOptions.IdentifierPolicy.<Language, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
						.usingConstructor(Language::new, "id", "code")
						.map(Language::getCode).setByConstructor()
				)
				.build(persistenceContext);
		entityPersister = asInternalPersister(persister);
	}

	@Test
	void extract_withClosedProjection() {
		SpelAwareProxyProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();
		ProjectionTypeInformationExtractor<Republic> testInstance = new ProjectionTypeInformationExtractor<>(projectionFactory, entityPersister);
		IdentityHashMap<JoinLink<?, ?>, PropertyPath> selectablePaths = testInstance.extract(NamesOnly.class);
		// we map the result to an ugly structure to be able to assert it easily because PropertyPath can't be created outside of Spring package
		// and JoinLinks can't be found easily.
		List<List<Object>> actual = Iterables.collectToList(selectablePaths.entrySet(), entry -> Arrays.asList(
				// column info
				entry.getKey().getOwner().getName(), entry.getKey().getExpression(), entry.getKey().getJavaType(),
				// propertyPath info
				entry.getValue().toDotPath(), entry.getValue().getType())
		);
		assertThat(actual).containsExactlyInAnyOrder(
				Arrays.asList("Person", "name", String.class, "president.name", Person.class),
				Arrays.asList("Republic", "name", String.class, "name", String.class)
		);
	}

	@Test
	void extract_withOpenProjection() {
		SpelAwareProxyProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();
		ProjectionTypeInformationExtractor<Republic> testInstance = new ProjectionTypeInformationExtractor<>(projectionFactory, entityPersister);
		IdentityHashMap<JoinLink<?, ?>, PropertyPath> selectablePaths = testInstance.extract(NamesOnlyWithValue.class);
		// we map the result to an ugly structure to be able to assert it easily because PropertyPath can't be created outside of Spring package
		// and JoinLinks can't be found easily.
		List<List<Object>> actual = Iterables.collectToList(selectablePaths.entrySet(), entry -> Arrays.asList(
				// column info
				entry.getKey().getOwner().getName(), entry.getKey().getExpression(), entry.getKey().getJavaType(),
				// propertyPath info
				entry.getValue().toDotPath(), entry.getValue().getType())
		);
		assertThat(actual).isEmpty();
	}
	
	@Test
	void analysis_PersistentEntity_getPersistentProperty() {

		EntityProjectionIntrospector.ProjectionPredicate isProjectionTest = (returnType, domainType)
				-> (!domainType.isAssignableFrom(returnType) && !returnType.isAssignableFrom(domainType));

//		RelationalMappingContext mappingContext = new RelationalMappingContext();
		MappingContext mappingContext = mock(MappingContext.class);
		BasicPersistentEntity<Republic, ?> ps = new BasicPersistentEntity<>(ClassTypeInformation.from(Republic.class), null);
		when(mappingContext.getPersistentEntity(Republic.class)).thenReturn(ps);
		EntityProjectionIntrospector entityProjectionIntrospector = EntityProjectionIntrospector.create(new SpelAwareProxyProjectionFactory(), isProjectionTest, mappingContext);
		EntityProjection<Republic, Republic> introspect = entityProjectionIntrospector.introspect(Republic.class, Republic.class);
		System.out.println(introspect);
	}

	interface NamesOnly {

		String getName();

		SimplePerson getPresident();

		interface SimplePerson {

			String getName();

		}
	}

	// This class should create a query that retrieves the whole aggregate because it has a @Value annotation
	// Indeed, values required by the @Value annotations can't be known in advance, so the query must retrieve the whole aggregate
	interface NamesOnlyWithValue extends DerivedQueriesRepository.NamesOnly {

		@Value("#{target.president.name + '-' + target.president.id.delegate}")
		String getPresidentName();
	}

}