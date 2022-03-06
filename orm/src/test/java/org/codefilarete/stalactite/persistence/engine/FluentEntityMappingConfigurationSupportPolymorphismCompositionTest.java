package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportPolymorphismTest.ElectricCar;
import org.codefilarete.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportPolymorphismTest.ElectricPlug;
import org.codefilarete.stalactite.persistence.engine.PersistenceContext.ExecutableBeanPropertyQueryMapper;
import org.codefilarete.stalactite.persistence.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.persistence.engine.model.Car;
import org.codefilarete.stalactite.persistence.engine.model.Color;
import org.codefilarete.stalactite.persistence.id.Identifier;
import org.codefilarete.stalactite.persistence.id.PersistedIdentifier;
import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.persistence.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.persistence.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportPolymorphismCompositionTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getSqlTypeRegistry().put(Color.class, "int");
	}
	
	@BeforeEach
	public void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
	}
	
	@Test
	void joinedTables_joinedTables() {
		EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
				// mapped super class defines id
				.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
						.addSubClass(subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel)
								.map(Car::getColor)
								// A second level of polymorphism
								.mapPolymorphism(PolymorphismPolicy.<Car>joinTable()
										.addSubClass(subentityBuilder(ElectricCar.class)
												.map(ElectricCar::getPlug)))
						))
				.build(persistenceContext);
		
		// Schema contains main and children tables
		HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
		assertThat(tables).isEqualTo(Arrays.asHashSet("AbstractVehicle", "Car", "ElectricCar"));
		
		// Subclasses are not present in context (because they have wrong behavior since some elements are configured on parent's persister)
		assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		ElectricCar dummyCar = new ElectricCar(1L);
		dummyCar.setModel("Renault");
		dummyCar.setColor(new Color(666));
		dummyCar.setPlug(ElectricPlug.CCS);
		
		// insert test
		abstractVehiclePersister.insert(dummyCar);
		
		ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle left outer join car on abstractVehicle.id = car.id", String.class)
				.mapKey("model", String.class);
		
		List<String> allCars = modelQuery.execute();
		assertThat(allCars).isEqualTo(Arrays.asList("Renault"));
		
		// update test
		dummyCar.setModel("Peugeot");
		abstractVehiclePersister.persist(dummyCar);
		
		List<String> existingModels = modelQuery.execute();
		assertThat(existingModels).isEqualTo(Arrays.asList("Peugeot"));
		
		// select test
		AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
		assertThat(loadedCar).isEqualTo(dummyCar);
		
		// delete test
		abstractVehiclePersister.delete(dummyCar);
		
		existingModels = modelQuery.execute();
		assertThat(existingModels).isEqualTo(Collections.emptyList());
	}
	
	
	@Test
	void joinedTables_singleTable() {
		EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
				// mapped super class defines id
				.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
						.addSubClass(MappingEase.<Car, Identifier<Long>>subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel)
								.map(Car::getColor)
								// A second level of polymorphism
								.mapPolymorphism(PolymorphismPolicy.<Car>singleTable()
										.addSubClass(subentityBuilder(ElectricCar.class)
												.map(ElectricCar::getPlug), "CAR")))
				)
				.build(persistenceContext);
		
		// Schema contains only one table : parent class one
		HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
		assertThat(tables).isEqualTo(Arrays.asHashSet("Car", "AbstractVehicle"));
		
		// Subclasses are not present in context (because they have wrong behavior since some elements are configured on parent's persister)
		assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		ElectricCar dummyCar = new ElectricCar(1L);
		dummyCar.setModel("Renault");
		dummyCar.setColor(new Color(666));
		dummyCar.setPlug(ElectricPlug.CCS);
		
		// insert test
		abstractVehiclePersister.insert(dummyCar);
		
		ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from car", String.class)
				.mapKey("model", String.class);
		
		List<String> allCars = modelQuery.execute();
		assertThat(allCars).isEqualTo(Arrays.asList("Renault"));
		
		// update test
		dummyCar.setModel("Peugeot");
		abstractVehiclePersister.persist(dummyCar);
		
		List<String> existingModels = modelQuery.execute();
		assertThat(existingModels).isEqualTo(Arrays.asList("Peugeot"));
		
		// select test
		AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
		assertThat(loadedCar).isEqualTo(dummyCar);
		
		// delete test
		abstractVehiclePersister.delete(dummyCar);
		
		existingModels = modelQuery.execute();
		assertThat(existingModels).isEqualTo(Collections.emptyList());
	}
	
	@Test
	void joinedTables_tablePerClass_isNotSupported() {
		FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> builder = entityBuilder(AbstractVehicle.class, LONG_TYPE)
				// mapped super class defines id
				.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
						.addSubClass(MappingEase.<Car, Identifier<Long>>subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel)
								.map(Car::getColor)
								// A second level of polymorphism
								.mapPolymorphism(PolymorphismPolicy.<Car>tablePerClass()
										.addSubClass(subentityBuilder(ElectricCar.class)
												.map(ElectricCar::getPlug))))
				);
		assertThatThrownBy(() -> builder.build(persistenceContext))
				.extracting(t -> Exceptions.findExceptionInCauses(t, NotImplementedException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Combining joined-tables polymorphism policy with o.c.s.p.e.PolymorphismPolicy$TablePerClassPolymorphism");
	}
	
	@Test
	void singleTable_joinedTables() {
		EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
				// mapped super class defines id
				.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
						.addSubClass(MappingEase.<Car, Identifier<Long>>subentityBuilder(Car.class)
										.map(Car::getId)
										.map(Car::getModel)
										.map(Car::getColor)
										// A second level of polymorphism
										.mapPolymorphism(PolymorphismPolicy.<Car>joinTable()
												.addSubClass(subentityBuilder(ElectricCar.class)
														.map(ElectricCar::getPlug)))
								, "CAR"))
				.build(persistenceContext);
		
		// Schema contains only one table : parent class one
		HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
		assertThat(tables).isEqualTo(Arrays.asHashSet("ElectricCar", "AbstractVehicle"));
		
		// Subclasses are not present in context (because they have wrong behavior since some elements are configured on parent's persister)
		assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		ElectricCar dummyCar = new ElectricCar(1L);
		dummyCar.setModel("Renault");
		dummyCar.setColor(new Color(666));
		dummyCar.setPlug(ElectricPlug.CCS);
		
		// insert test
		abstractVehiclePersister.insert(dummyCar);
		
		ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle", String.class)
				.mapKey("model", String.class);
		
		List<String> allCars = modelQuery.execute();
		assertThat(allCars).isEqualTo(Arrays.asList("Renault"));
		
		// update test
		dummyCar.setModel("Peugeot");
		abstractVehiclePersister.persist(dummyCar);
		
		List<String> existingModels = modelQuery.execute();
		assertThat(existingModels).isEqualTo(Arrays.asList("Peugeot"));
		
		// select test
		AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
		assertThat(loadedCar).isEqualTo(dummyCar);
		
		// delete test
		abstractVehiclePersister.delete(dummyCar);
		
		existingModels = modelQuery.execute();
		assertThat(existingModels).isEqualTo(Collections.emptyList());
	}
	
	@Test
	void singleTable_singleTable_isNotSupported() {
		FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> builder = entityBuilder(AbstractVehicle.class, LONG_TYPE)
				// mapped super class defines id
				.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
						.addSubClass(MappingEase.<Car, Identifier<Long>>subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel)
								.map(Car::getColor)
								// A second level of polymorphism
								.mapPolymorphism(PolymorphismPolicy.<Car>singleTable()
										.addSubClass(subentityBuilder(ElectricCar.class)
												.map(ElectricCar::getPlug), "ELECTRIC_CAR")), "CAR")
				);
		assertThatThrownBy(() -> builder.build(persistenceContext))
				.extracting(t -> Exceptions.findExceptionInCauses(t, NotImplementedException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Combining joined-tables polymorphism policy with o.c.s.p.e.PolymorphismPolicy$SingleTablePolymorphism");
	}
	
	@Test
	void singleTable_tablePerClass_isNotSupported() {
		FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> builder = entityBuilder(AbstractVehicle.class, LONG_TYPE)
				// mapped super class defines id
				.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
						.addSubClass(MappingEase.<Car, Identifier<Long>>subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel)
								.map(Car::getColor)
								// A second level of polymorphism
								.mapPolymorphism(PolymorphismPolicy.<Car>tablePerClass()
										.addSubClass(subentityBuilder(ElectricCar.class)
												.map(ElectricCar::getPlug))), "CAR")
				);
		assertThatThrownBy(() -> builder.build(persistenceContext))
				.extracting(t -> Exceptions.findExceptionInCauses(t, NotImplementedException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Combining joined-tables polymorphism policy with o.c.s.p.e.PolymorphismPolicy$TablePerClassPolymorphism");
	}
}