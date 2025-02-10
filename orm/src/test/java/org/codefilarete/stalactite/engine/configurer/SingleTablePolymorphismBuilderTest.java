package org.codefilarete.stalactite.engine.configurer;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.*;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.model.*;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.Sequence;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.engine.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

/**
 * @author Guillaume Mary
 */
class SingleTablePolymorphismBuilderTest {
	
	public static final SingleTablePolymorphism<Element, String> POLYMORPHISM_POLICY = PolymorphismPolicy.<Element>singleTable()
			.addSubClass(subentityBuilder(Question.class)
					.map(Question::getLabel), "QUESTION")
			.addSubClass(subentityBuilder(Part.class)
					.map(Part::getName), "PART");
	
	@Test
	void build_targetTableAndOverridingColumnsAreDifferent_throwsException() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		PersistenceContext persistenceContext = new PersistenceContext(Mockito.mock(ConnectionProvider.class), dialect);
		
		Table expectedResult = new Table("MyOverridingTable");
		Column colorTable = expectedResult.addColumn("myOverridingColumn", Integer.class);
		
		FluentEntityMappingBuilder<Vehicle, Identifier<Long>> configuration = entityBuilder(Vehicle.class, LONG_TYPE, new Table("TargetTable"))
				.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
						.addSubClass(subentityBuilder(Car.class)
								.map(Car::getModel)
								.embed(Vehicle::getColor, embeddableBuilder(Color.class)
										.map(Color::getRgb)).override(Color::getRgb, colorTable), "CAR"));
		
		
		assertThatThrownBy(() -> configuration.build(persistenceContext))
				.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Table declared in inheritance is different from given one in embeddable properties override : MyOverridingTable, TargetTable");
	}
	
	@Test
	void build_withAlreadyAssignedIdentifierPolicy_entitiesMustHaveTheirIdSet() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		EntityPersister<Element, Long> configuration = entityBuilder(Element.class, long.class)
				.mapKey(Element::getId, IdentifierPolicy.alreadyAssigned(c -> {}, c -> false))
				.mapPolymorphism(POLYMORPHISM_POLICY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question theUltimateQuestion = new Question(1).setLabel("What's the answer to Life, the Universe and Everything ?");
		configuration.persist(Arrays.asList(theUltimateQuestion));
		
		assertThat(theUltimateQuestion.getId()).isEqualTo(1);
	}
	
	@Test
	void build_withAfterInsertIdentifierPolicy_entitiesMustHaveTheirIdSet() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		EntityPersister<Element, Long> configuration = entityBuilder(Element.class, long.class)
				.mapKey(Element::getId, IdentifierPolicy.afterInsert())
				.mapPolymorphism(POLYMORPHISM_POLICY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Question theUltimateQuestion = new Question().setLabel("What's the answer to Life, the Universe and Everything ?");
		configuration.persist(Arrays.asList(theUltimateQuestion));
		
		assertThat(theUltimateQuestion.getId()).isEqualTo(1);
	}
	
	@Test
	void build_withBeforeInsertIdentifierPolicy_entitiesMustHaveTheirIdSet() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		
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
		configuration.persist(Arrays.asList(theUltimateQuestion));
		
		assertThat(theUltimateQuestion.getId()).isEqualTo(1);
	}
}