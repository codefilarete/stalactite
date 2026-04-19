package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.MethodReferences;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceMappingResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.engine.configurer.resolver.InheritanceMappingResolver.IDENTIFIER_METHOD_REFERENCE;
import static org.mockito.Mockito.mock;

class InheritanceMappingResolverTest {
	
	@Nested
	class Collect {
		
		@Test
		void collect_direct() {
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
					.mapKey(E::getPropE, IdentifierPolicy.databaseAutoIncrement());
			
			InheritanceMappingResolver<E, Integer> testInstance = new InheritanceMappingResolver<>();
			KeepOrderSet<ResolvedConfiguration<?, Integer>> configurations = testInstance.resolveConfigurations(entityMappingBuilder.getConfiguration());
			ResolvedConfiguration<?, Integer> eResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == E.class);
			assertThat(eResolvedConfiguration.getTable().getName()).isEqualTo("E");
			assertThat(eResolvedConfiguration.getKeyMapping()).isInstanceOf(EntityMappingConfiguration.SingleKeyMapping.class);
			assertThat(eResolvedConfiguration.getKeyMapping().getAccessor().getReader()).isEqualTo(new AccessorByMethodReference<>(E::getPropE));
			assertThat(eResolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy()).isSameAs(ColumnNamingStrategy.DEFAULT);
			assertThat(eResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(TableNamingStrategy.DEFAULT);
		}
		
		@Test
		void collect_overridden() {
			ColumnNamingStrategy myColumnStrategy1 = mock(ColumnNamingStrategy.class);
			ColumnNamingStrategy myColumnStrategy2 = mock(ColumnNamingStrategy.class);
			
			TableNamingStrategy myTableStrategy1 = persistedClass -> "table1";
			TableNamingStrategy myTableStrategy2 = persistedClass -> "table2";
			
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
					.map(E::getPropE)
					.mapSuperClass(entityBuilder(D.class, int.class)
							.map(D::getPropD)
							.withColumnNaming(myColumnStrategy1)
							.withTableNaming(myTableStrategy1)
							.mapSuperClass(entityBuilder(C.class, int.class)
									.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
									.mapSuperClass(entityBuilder(B.class, int.class)
											.map(B::getPropB)
											.withColumnNaming(myColumnStrategy2)
											.withTableNaming(myTableStrategy2)
											.mapSuperClass(entityBuilder(A.class, int.class)
													.map(A::getPropA)
											)
									)
							)
					);
			
			InheritanceMappingResolver<E, Integer> testInstance = new InheritanceMappingResolver<>();
			KeepOrderSet<ResolvedConfiguration<?, Integer>> configurations = testInstance.resolveConfigurations(entityMappingBuilder.getConfiguration());
			
			ResolvedConfiguration<?, Integer> eResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == E.class);
			assertThat(eResolvedConfiguration.getTable().getName()).isEqualTo("table1");
			assertThat(eResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(eResolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy()).isSameAs(myColumnStrategy1);
			assertThat(eResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy1);
			
			ResolvedConfiguration<?, Integer> dResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == D.class);
			assertThat(dResolvedConfiguration.getTable().getName()).isEqualTo("table1");
			assertThat(dResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(dResolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy()).isSameAs(myColumnStrategy1);
			assertThat(dResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy1);
			
			ResolvedConfiguration<?, Integer> cResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == C.class);
			assertThat(cResolvedConfiguration.getTable().getName()).isEqualTo("table1");
			assertThat(cResolvedConfiguration.getKeyMapping()).isInstanceOf(EntityMappingConfiguration.SingleKeyMapping.class);
			assertThat(cResolvedConfiguration.getKeyMapping().getAccessor().getReader()).isEqualTo(new AccessorByMethodReference<>(C::getPropC));
			assertThat(cResolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy()).isSameAs(myColumnStrategy2);
			assertThat(cResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> bResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == B.class);
			assertThat(bResolvedConfiguration.getTable().getName()).isEqualTo("table1");
			assertThat(bResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(bResolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy()).isSameAs(myColumnStrategy2);
			assertThat(bResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> aResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == A.class);
			assertThat(aResolvedConfiguration.getTable().getName()).isEqualTo("table1");
			assertThat(aResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(aResolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy()).isSameAs(ColumnNamingStrategy.DEFAULT);
			assertThat(aResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(TableNamingStrategy.DEFAULT);
		}
		
		@Test
		void collect_overridden_joiningTables() {
			TableNamingStrategy myTableStrategy1 = persistedClass -> "myTable1";
			TableNamingStrategy myTableStrategy2 = persistedClass -> "myTable2";
			
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
					.map(E::getPropE)
					.mapSuperClass(entityBuilder(D.class, int.class)
							.map(D::getPropD)
							.withTableNaming(myTableStrategy1)
							.mapSuperClass(entityBuilder(C.class, int.class)
									.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
									.mapSuperClass(entityBuilder(B.class, int.class)
											.map(B::getPropB)
											.withTableNaming(myTableStrategy2)
											.mapSuperClass(entityBuilder(A.class, int.class)
													.map(A::getPropA)
											).joiningTables()
									)
							).joiningTables()
					);
			
			InheritanceMappingResolver<E, Integer> testInstance = new InheritanceMappingResolver<>();
			KeepOrderSet<ResolvedConfiguration<?, Integer>> configurations = testInstance.resolveConfigurations(entityMappingBuilder.getConfiguration());
			
			ResolvedConfiguration<?, Integer> eResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == E.class);
			assertThat(eResolvedConfiguration.getTable().getName()).isEqualTo("myTable1");
			assertThat(eResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(eResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy1);
			
			ResolvedConfiguration<?, Integer> dResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == D.class);
			assertThat(dResolvedConfiguration.getTable().getName()).isEqualTo("myTable1");
			assertThat(dResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(dResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy1);
			
			ResolvedConfiguration<?, Integer> cResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == C.class);
			assertThat(cResolvedConfiguration.getTable().getName()).isEqualTo("myTable2");
			assertThat(cResolvedConfiguration.getKeyMapping()).isInstanceOf(EntityMappingConfiguration.SingleKeyMapping.class);
			assertThat(cResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> bResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == B.class);
			assertThat(bResolvedConfiguration.getTable().getName()).isEqualTo("myTable2");
			assertThat(bResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(bResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> aResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == A.class);
			assertThat(aResolvedConfiguration.getTable().getName()).isEqualTo("A");
			assertThat(aResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(aResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(TableNamingStrategy.DEFAULT);
		}
		
		@Test
		void collect_overridden_joiningTables_onTable() {
			TableNamingStrategy myTableStrategy1 = persistedClass -> "myTable1";
			TableNamingStrategy myTableStrategy2 = persistedClass -> "myTable2";
			
			Table myTable1 = new Table("myTable1");
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
					.map(E::getPropE)
					.onTable(myTable1)
					.mapSuperClass(entityBuilder(D.class, int.class)
							.map(D::getPropD)
							.mapSuperClass(entityBuilder(C.class, int.class)
									.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
									.mapSuperClass(entityBuilder(B.class, int.class)
											.map(B::getPropB)
											.withTableNaming(myTableStrategy2)
											.mapSuperClass(entityBuilder(A.class, int.class)
													.map(A::getPropA)
											).joiningTables()
									)
							).joiningTables()
					);
			
			InheritanceMappingResolver<E, Integer> testInstance = new InheritanceMappingResolver<>();
			KeepOrderSet<ResolvedConfiguration<?, Integer>> configurations = testInstance.resolveConfigurations(entityMappingBuilder.getConfiguration());
			
			ResolvedConfiguration<?, Integer> eResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == E.class);
			assertThat(eResolvedConfiguration.getTable()).isSameAs(myTable1);
			assertThat(eResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(eResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> dResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == D.class);
			assertThat(dResolvedConfiguration.getTable()).isSameAs(myTable1);
			assertThat(dResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(dResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> cResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == C.class);
			assertThat(cResolvedConfiguration.getTable().getName()).isEqualTo("myTable2");
			assertThat(cResolvedConfiguration.getKeyMapping()).isInstanceOf(EntityMappingConfiguration.SingleKeyMapping.class);
			assertThat(cResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> bResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == B.class);
			assertThat(bResolvedConfiguration.getTable().getName()).isEqualTo("myTable2");
			assertThat(bResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(bResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(myTableStrategy2);
			
			ResolvedConfiguration<?, Integer> aResolvedConfiguration = Iterables.find(configurations, c -> c.getMappingConfiguration().getEntityType() == A.class);
			assertThat(aResolvedConfiguration.getTable().getName()).isEqualTo("A");
			assertThat(aResolvedConfiguration.getKeyMapping()).isNull();
			assertThat(aResolvedConfiguration.getNamingConfiguration().getTableNamingStrategy()).isSameAs(TableNamingStrategy.DEFAULT);
		}
	}
	
	@Nested
	class MisconfigurationHandling {
		
		@Test
		void noKeyIsDefined_throwsException() {
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
					.map(E::getPropE)
					.mapSuperClass(entityBuilder(D.class, int.class)
							.map(D::getPropD)
							.mapSuperClass(entityBuilder(C.class, int.class)
									.map(C::getPropC)
									.mapSuperClass(entityBuilder(B.class, int.class)
											.map(B::getPropB)
											.mapSuperClass(entityBuilder(A.class, int.class)
													.map(A::getPropA)
											)
									)
							)
					);
			
			InheritanceMappingResolver<E, Integer> testInstance = new InheritanceMappingResolver<>();
			assertThatCode(() -> testInstance.resolveConfigurations(entityMappingBuilder.getConfiguration()))
					.isInstanceOf(UnsupportedOperationException.class)
					.hasMessageContaining("Identifier is not defined for o.c.s.e.c.r.InheritanceMappingResolverTest$E, please add one through " +
							MethodReferences.toMethodReferenceString(IDENTIFIER_METHOD_REFERENCE) + " variants");
		}
		
		@Test
		void keyIsDefinedTwice_throwsException() {
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
					.map(E::getPropE)
					.mapSuperClass(entityBuilder(D.class, int.class)
							.map(D::getPropD)
							.mapSuperClass(entityBuilder(C.class, int.class)
									.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
									.mapSuperClass(entityBuilder(B.class, int.class)
											.mapKey(B::getPropB, IdentifierPolicy.databaseAutoIncrement())
											.mapSuperClass(entityBuilder(A.class, int.class)
													.map(A::getPropA)
											)
									)
							)
					);
			
			InheritanceMappingResolver<E, Integer> testInstance = new InheritanceMappingResolver<>();
			assertThatCode(() -> testInstance.resolveConfigurations(entityMappingBuilder.getConfiguration()))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessageContaining("Identifier policy is defined twice in the hierarchy : first by B::getPropB, then by C::getPropC");
		}
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
