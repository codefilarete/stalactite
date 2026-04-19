package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.mockito.Mockito.mock;

class NamingConfigurationCollectorTest {
	
	@Test
	void collect_direct() {
		ColumnNamingStrategy myStrategy = mock(ColumnNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.mapKey(E::getPropE, IdentifierPolicy.databaseAutoIncrement())
				.withColumnNaming(myStrategy);
		
		NamingConfigurationCollector testInstance = new NamingConfigurationCollector(entityMappingBuilder.getConfiguration());
		NamingConfiguration namingConfiguration = testInstance.collect();
		assertThat(namingConfiguration.getColumnNamingStrategy()).isSameAs(myStrategy);
	}
	
	@Test
	void collect_upperLevel() {
		ColumnNamingStrategy myStrategy = mock(ColumnNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.map(E::getPropE)
				.mapSuperClass(entityBuilder(D.class, int.class)
						.mapKey(D::getPropD, IdentifierPolicy.databaseAutoIncrement())
						.withColumnNaming(myStrategy));
		
		NamingConfigurationCollector testInstance = new NamingConfigurationCollector(entityMappingBuilder.getConfiguration());
		NamingConfiguration namingConfiguration = testInstance.collect();
		assertThat(namingConfiguration.getColumnNamingStrategy()).isSameAs(myStrategy);
	}
	
	@Test
	void collect_highestLevel() {
		ColumnNamingStrategy myStrategy = mock(ColumnNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.map(E::getPropE)
				.mapSuperClass(entityBuilder(D.class, int.class)
						.map(D::getPropD)
						.mapSuperClass(entityBuilder(C.class, int.class)
								.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
								.withColumnNaming(myStrategy)));
		
		NamingConfigurationCollector testInstance = new NamingConfigurationCollector(entityMappingBuilder.getConfiguration());
		NamingConfiguration namingConfiguration = testInstance.collect();
		assertThat(namingConfiguration.getColumnNamingStrategy()).isSameAs(myStrategy);
	}
	
	@Test
	void collect_highestLevel_overridden() {
		ColumnNamingStrategy myStrategy1 = mock(ColumnNamingStrategy.class);
		ColumnNamingStrategy myStrategy2 = mock(ColumnNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.map(E::getPropE)
				.mapSuperClass(entityBuilder(D.class, int.class)
						.map(D::getPropD)
						.withColumnNaming(myStrategy1)
						.mapSuperClass(entityBuilder(C.class, int.class)
								.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
								.withColumnNaming(myStrategy2)));
		
		NamingConfigurationCollector testInstance = new NamingConfigurationCollector(entityMappingBuilder.getConfiguration());
		NamingConfiguration namingConfiguration = testInstance.collect();
		assertThat(namingConfiguration.getColumnNamingStrategy()).isSameAs(myStrategy1);
	}
	
	@Test
	void collect_upperLevel_embeddable() {
		ColumnNamingStrategy myStrategy1 = mock(ColumnNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.map(E::getPropE)
				.mapSuperClass(embeddableBuilder(D.class)
						.map(D::getPropD)
						.withColumnNaming(myStrategy1));
		
		NamingConfigurationCollector testInstance = new NamingConfigurationCollector(entityMappingBuilder.getConfiguration());
		NamingConfiguration namingConfiguration = testInstance.collect();
		assertThat(namingConfiguration.getColumnNamingStrategy()).isSameAs(myStrategy1);
	}
	
	@Test
	void collect_upperLevel_entity_embeddable() {
		ColumnNamingStrategy myStrategy1 = mock(ColumnNamingStrategy.class);
		ColumnNamingStrategy myStrategy2 = mock(ColumnNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.map(E::getPropE)
				.mapSuperClass(entityBuilder(D.class, int.class)
						.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
						.withColumnNaming(myStrategy1)
						.mapSuperClass(embeddableBuilder(C.class)
								.map(C::getPropC)
								.withColumnNaming(myStrategy2)));
		
		NamingConfigurationCollector testInstance = new NamingConfigurationCollector(entityMappingBuilder.getConfiguration());
		NamingConfiguration namingConfiguration = testInstance.collect();
		assertThat(namingConfiguration.getColumnNamingStrategy()).isSameAs(myStrategy1);
	}
	
	@Test
	void collect_highestLevel_entity_embeddable() {
		ColumnNamingStrategy myStrategy1 = mock(ColumnNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.map(E::getPropE)
				.mapSuperClass(entityBuilder(D.class, int.class)
						.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
						.mapSuperClass(embeddableBuilder(C.class)
								.map(C::getPropC)
								.withColumnNaming(myStrategy1)));
		
		NamingConfigurationCollector testInstance = new NamingConfigurationCollector(entityMappingBuilder.getConfiguration());
		NamingConfiguration namingConfiguration = testInstance.collect();
		assertThat(namingConfiguration.getColumnNamingStrategy()).isSameAs(myStrategy1);
	}
	
	@Test
	void test_forIATip() {
		TableNamingStrategy anyTableNaming1 = mock(TableNamingStrategy.class);
		TableNamingStrategy anyTableNaming2 = mock(TableNamingStrategy.class);
		
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.map(E::getPropE)
				.withTableNaming(anyTableNaming1)
				.mapSuperClass(entityBuilder(D.class, int.class)
						.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
						.mapSuperClass(entityBuilder(C.class, int.class)
								.withTableNaming(anyTableNaming2)
								.map(C::getPropC)));
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
	}
	
	private static class C extends B {
		
		private int propC;
		
		public int getPropC() {
			return propC;
		}
	}
	
	private static class D extends C {
		
		private int propD;
		
		public int getPropD() {
			return propD;
		}
	}
	
	private static class E extends D {
		
		private int propE;
		
		public int getPropE() {
			return propE;
		}
	}
}
