package org.codefilarete.stalactite.engine.configurer;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
class JoinTablePolymorphismBuilderTest {
	
	@Test
	void build_targetTableAndOverringColumnsAreDifferent_throwsException() {
		HSQLDBDialect dialect = new HSQLDBDialect();
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
				.hasMessage("Table declared in inheritance is different from given one in embeddable properties override : MyOverridingTable, TargetTable");
	}
}