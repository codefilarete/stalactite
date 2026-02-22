package org.codefilarete.stalactite.engine.configurer;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.survey.Element;
import org.codefilarete.stalactite.engine.model.survey.Part;
import org.codefilarete.stalactite.engine.model.survey.Question;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.Sequence;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

/**
 * @author Guillaume Mary
 */
class JoinTablePolymorphismBuilderTest {
	
	public static final JoinTablePolymorphism<Element> POLYMORPHISM_POLICY = PolymorphismPolicy.joinTable(Element.class)
			.addSubClass(subentityBuilder(Question.class)
					.map(Question::getLabel))
			.addSubClass(subentityBuilder(Part.class)
					.map(Part::getName));
	
	
	@Test
	void build_targetTableAndOverridingColumnsAreDifferent_throwsException() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		PersistenceContext persistenceContext = new PersistenceContext(Mockito.mock(ConnectionProvider.class), dialect);
		
		Table expectedResult = new Table("MyOverridingTable");
		Column colorTable = expectedResult.addColumn("myOverridingColumn", Integer.class);
		
		FluentEntityMappingBuilder<Vehicle, Identifier<Long>> configuration = entityBuilder(Vehicle.class, LONG_TYPE)
				.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
						.addSubClass(subentityBuilder(Car.class)
								.map(Car::getModel)
								.embed(Vehicle::getColor, embeddableBuilder(Color.class)
										.map(Color::getRgb))
								.override(Color::getRgb, colorTable), new Table("TargetTable")));
		
		
		assertThatThrownBy(() -> configuration.build(persistenceContext))
				.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Property o.c.s.e.m.Color::getRgb overrides column with MyOverridingTable.myOverridingColumn but it is not part of main table TargetTable");
	}
	
	@Test
	void build_withAlreadyAssignedIdentifierPolicy_entitiesMustHaveTheirIdSet() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		EntityPersister<Element, Long> configuration = entityBuilder(Element.class, long.class)
				.mapKey(Element::getId, IdentifierPolicy.alreadyAssigned(c -> {}, c -> false))
				.mapPolymorphism(POLYMORPHISM_POLICY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question theUltimateQuestion = new Question(1).setLabel("What's the answer to Life, the Universe and Everything ?");
		configuration.persist(theUltimateQuestion);
		
		assertThat(theUltimateQuestion.getId()).isEqualTo(1);
	}
	
	@Test
	void build_withAfterInsertIdentifierPolicy_entitiesMustHaveTheirIdSet() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		EntityPersister<Element, Long> configuration = entityBuilder(Element.class, long.class)
				.mapKey(Element::getId, IdentifierPolicy.databaseAutoIncrement())
				.mapPolymorphism(POLYMORPHISM_POLICY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question theUltimateQuestion = new Question().setLabel("What's the answer to Life, the Universe and Everything ?");
		configuration.persist(theUltimateQuestion);
		
		assertThat(theUltimateQuestion.getId()).isEqualTo(1);
	}
	
	@Test
	void build_withBeforeInsertIdentifierPolicy_entitiesMustHaveTheirIdSet() {
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		Sequence<Long> identifierGenerator = new Sequence<Long>() {
			
			private long counter = 0;
			
			@Override
			public Long next() {
				return ++counter;
			}
		};
		EntityPersister<Element, Long> configuration = entityBuilder(Element.class, long.class)
				.mapKey(Element::getId, IdentifierPolicy.pooledHiLoSequence(identifierGenerator))
				.mapPolymorphism(POLYMORPHISM_POLICY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question theUltimateQuestion = new Question().setLabel("What's the answer to Life, the Universe and Everything ?");
		configuration.persist(theUltimateQuestion);
		
		assertThat(theUltimateQuestion.getId()).isEqualTo(1);
	}
}
