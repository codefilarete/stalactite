package org.gama.stalactite.persistence.engine.configurer;

import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.engine.IFluentEntityMappingBuilder;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.model.AbstractVehicle;
import org.gama.stalactite.persistence.engine.model.Car;
import org.gama.stalactite.persistence.engine.model.Color;
import org.gama.stalactite.persistence.engine.model.Vehicle;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.persistence.engine.MappingEase.subentityBuilder;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.persistence.id.Identifier.identifierBinder;
import static org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

/**
 * @author Guillaume Mary
 */
class JoinedTablesPolymorphismBuilderTest {
	
	@Test
	void build_targetTableAndOverringColumnsAreDifferent_throwsException() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		PersistenceContext persistenceContext = new PersistenceContext(Mockito.mock(ConnectionProvider.class), dialect);
		
		Table expectedResult = new Table("MyOverridingTable");
		Column colorTable = expectedResult.addColumn("myOverridingColumn", Integer.class);
		
		IFluentEntityMappingBuilder<Vehicle, Identifier<Long>> configuration = entityBuilder(Vehicle.class, LONG_TYPE)
				.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinedTables()
						.addSubClass(subentityBuilder(Car.class)
								.add(Car::getModel)
								.embed(Vehicle::getColor).override(Color::getRgb, colorTable), new Table("TargetTable")));
		
		
		Assertions.assertThrows(() -> configuration.build(persistenceContext), 
				Assertions.hasExceptionInCauses(MappingConfigurationException.class)
		.andProjection(Assertions.hasMessage("Table declared in inheritance is different from given one in embeddable properties override : MyOverridingTable, TargetTable")));
	}
}