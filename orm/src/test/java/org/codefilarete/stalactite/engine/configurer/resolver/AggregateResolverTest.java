package org.codefilarete.stalactite.engine.configurer.resolver;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.compositeKeyBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;

class AggregateResolverTest {
	
	@Test
	void resolve_oneEntity() {
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.mapKey(E::getPropE, IdentifierPolicy.databaseAutoIncrement())
				.map(E::getPropD);
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, Integer> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getPropE());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getPropE());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
	}
	
	@Test
	void resolve_entityWithInheritance_entityAndEmbeddableInHierarchy_noJoiningTables() {
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.mapKey(E::getPropE, IdentifierPolicy.databaseAutoIncrement())
				.mapSuperClass(embeddableBuilder(D.class)
						.map(D::getPropD)
						.map(B::getPropB)
						.map(A::getPropA)
				);
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, Integer> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getPropE());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getPropE());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
	}
	
	@Test
	void resolve_entityWithInheritance_onlyEntityInHierarchy_noJoiningTables() {
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder =
				entityBuilder(E.class, int.class)
						.map(E::getPropE)
						.mapSuperClass(entityBuilder(D.class, int.class)
								.map(D::getPropD)
								.mapSuperClass(entityBuilder(C.class, int.class)
										.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
										.mapSuperClass(entityBuilder(B.class, int.class)
												.map(B::getPropB)
												.mapSuperClass(entityBuilder(A.class, int.class)
														.map(A::getPropA)
												)
										)
								)
						);
		
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, Integer> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getPropC());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getPropC());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
	}
	
	@Test
	void resolve_entityWithInheritance_onlyEntityInHierarchy_withJoiningTables() {
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder =
				entityBuilder(E.class, int.class)
						.map(E::getPropE)
						.mapSuperClass(entityBuilder(D.class, int.class)
								.map(D::getPropD)
								.mapSuperClass(entityBuilder(C.class, int.class)
										.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
										.mapSuperClass(entityBuilder(B.class, int.class)
												.map(B::getPropB)
												.mapSuperClass(entityBuilder(A.class, int.class)
														.map(A::getPropA)
												)
										)
								)
						).joiningTables();
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, Integer> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getPropC());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getPropC());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
		rowCount = persistenceContext.newQuery("select count(*) as count from D", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
	}
	
	@Test
	void resolve_entityWithInheritance_onlyEntityInHierarchy_withJoiningTables_withExtraTable() {
		Table extraTable1 = new Table("extraTable1");
		Table extraTable2 = new Table("extraTable2");
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder =
				entityBuilder(E.class, int.class)
						.map(E::getPropE).extraTable(extraTable1)
						.mapSuperClass(entityBuilder(D.class, int.class)
								.map(D::getPropD)
								.mapSuperClass(entityBuilder(C.class, int.class)
										.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
										.mapSuperClass(entityBuilder(B.class, int.class)
												.map(B::getPropB).extraTable(extraTable2)
												.mapSuperClass(entityBuilder(A.class, int.class)
														.map(A::getPropA)
												)
										)
								)
						).joiningTables();
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, Integer> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getPropC());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		// we set a value to the property stored on the extra table
		entity.setPropE(17);
		entity.setPropB(19);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getPropC());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
		rowCount = persistenceContext.newQuery("select count(*) as count from D", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
		
	}
	
	@Test
	void resolve_entityWithInheritance_onlyEntityInHierarchy_withJoiningTables_withCompositeKey() {
		FluentEntityMappingBuilder<E, CompositeId> entityMappingBuilder =
				entityBuilder(E.class, CompositeId.class)
						.map(E::getPropE)
						.mapSuperClass(entityBuilder(D.class, CompositeId.class)
								.map(D::getPropD)
								.mapSuperClass(entityBuilder(C.class, CompositeId.class)
										.mapKey(C::getCompositeId, compositeKeyBuilder(CompositeId.class)
												.map(CompositeId::getPropX)
												.map(CompositeId::getPropY)
										)
										.mapSuperClass(entityBuilder(B.class, CompositeId.class)
												.map(B::getPropB)
												.mapSuperClass(entityBuilder(A.class, CompositeId.class)
														.map(A::getPropA)
												)
										)
								)
						).joiningTables();
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, CompositeId> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entity.setCompositeId(new CompositeId("a", "b"));
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getCompositeId());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		entity.setPropE(17);
		entity.setPropB(19);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getCompositeId());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
		rowCount = persistenceContext.newQuery("select count(*) as count from D", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
	}
	
	@Test
	void resolve_entityWithInheritance_onlyEntityInHierarchy_withJoiningTables_withExtraTable_withCompositeKey() {
		Table extraTable1 = new Table("extraTable1");
		Table extraTable2 = new Table("extraTable2");
		
		FluentEntityMappingBuilder<E, CompositeId> entityMappingBuilder =
				entityBuilder(E.class, CompositeId.class)
						.map(E::getPropE).extraTable(extraTable1)
						.mapSuperClass(entityBuilder(D.class, CompositeId.class)
										.map(D::getPropD)
										.mapSuperClass(entityBuilder(C.class, CompositeId.class)
														.mapKey(C::getCompositeId, compositeKeyBuilder(CompositeId.class)
																.map(CompositeId::getPropX)
																.map(CompositeId::getPropY)
														)
														.mapSuperClass(entityBuilder(B.class, CompositeId.class)
																.map(B::getPropB).extraTable(extraTable2)
																.mapSuperClass(entityBuilder(A.class, CompositeId.class)
																		.map(A::getPropA)
																)
														)
										)
						).joiningTables();
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, CompositeId> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entity.setCompositeId(new CompositeId("a", "b"));
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getCompositeId());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		// we set a value to the property stored on the extra table
		entity.setPropE(17);
		entity.setPropB(19);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getCompositeId());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
		rowCount = persistenceContext.newQuery("select count(*) as count from D", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
	}
	
	@Test
	void resolve_oneEntity_withCompositeKey() {
		FluentEntityMappingBuilder<E, CompositeId> entityMappingBuilder = entityBuilder(E.class, CompositeId.class)
				.mapKey(E::getCompositeId, compositeKeyBuilder(CompositeId.class)
						.map(CompositeId::getPropX)
						.map(CompositeId::getPropY)
				)
				.map(E::getPropD);
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<E, CompositeId> entityPersister = testInstance.resolve(entityMappingBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		E entity = new E();
		entity.setCompositeId(new CompositeId("a", "b"));
		entityPersister.insert(entity);
		E entityClone = entityPersister.select(entity.getCompositeId());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entity.setPropD(42);
		entityPersister.update(entity);
		entityClone = entityPersister.select(entity.getCompositeId());
		assertThat(entityClone).usingRecursiveComparison().isEqualTo(entity);
		entityPersister.delete(entity);
		int rowCount = persistenceContext.newQuery("select count(*) as count from E", int.class)
				.mapKey("count", int.class)
				.execute(Accumulators.getFirst());
		assertThat(rowCount).isEqualTo(0);
	}
	
	@Test
	void multiple_oneToOne() throws SQLException {
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		
		persistenceContext.getDialect().getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		persistenceContext.getDialect().getSqlTypeRegistry().put(Identifier.class, "int");
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName);
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, ALREADY_ASSIGNED)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, personMappingBuilder).cascading(ALL)
				.mapOneToOne(Country::getCapital, cityMappingBuilder).cascading(ALL);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		City capital = new City(new LongProvider().giveNewIdentifier());
		capital.setName("Paris");
		dummyCountry.setCapital(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertThat(persistedCountry2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedCountry2.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry2.getPresident().getId().getDelegate()).isEqualTo(persistedCountry.getPresident().getId().getDelegate());
		assertThat(persistedCountry2.getCapital().getId().getDelegate()).isEqualTo(persistedCountry.getCapital().getId().getDelegate());
		assertThat(persistedCountry2.getPresident()).isNotSameAs(persistedCountry.getPresident());
		assertThat(persistedCountry2.getCapital()).isNotSameAs(persistedCountry.getCapital());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("French president renamed");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("Paris renamed");
		assertThat(resultSet.next()).isFalse();
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry2.getId().getDelegate())).isEqualTo(1);
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Country where id = " + persistedCountry.getId().getDelegate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Person where id = " + persistedCountry.getPresident().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from City where id = " + persistedCountry.getCapital().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
	}
	
	private static class A {
		
		private int propA;
		
		public int getPropA() {
			return propA;
		}
	}
	
	private static class B extends A {
		
		private int propB;
		
		public int getPropB() {
			return propB;
		}
		
		public void setPropB(int propB) {
			this.propB = propB;
		}
	}
	
	private static class C extends B {
		
		private int propC;
		
		private CompositeId compositeId;
		
		public int getPropC() {
			return propC;
		}
		
		public CompositeId getCompositeId() {
			return compositeId;
		}
		
		public void setCompositeId(CompositeId compositeId) {
			this.compositeId = compositeId;
		}
	}
	
	private static class D extends C {
		
		private int propD;
		
		public int getPropD() {
			return propD;
		}
		
		public void setPropD(int propD) {
			this.propD = propD;
		}
	}
	
	private static class E extends D {
		
		private int propE;
		
		public int getPropE() {
			return propE;
		}
		
		public void setPropE(int propE) {
			this.propE = propE;
		}
	}
	
	private static class CompositeId {
		
		private String propX;
		
		private String propY;
		
		public CompositeId() {
		}
		
		public CompositeId(String propX, String propY) {
			this.propX = propX;
			this.propY = propY;
		}
		
		public String getPropX() {
			return propX;
		}
		
		public String getPropY() {
			return propY;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o == null || getClass() != o.getClass()) return false;
			CompositeId that = (CompositeId) o;
			return Objects.equals(propX, that.propX) && Objects.equals(propY, that.propY);
		}

		@Override
		public int hashCode() {
			return Objects.hash(propX, propY);
		}
	}
}
