package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Objects;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.compositeKeyBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;

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
