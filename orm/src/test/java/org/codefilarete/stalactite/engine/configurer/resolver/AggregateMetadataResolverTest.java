package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.Entity.PropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.ExtraTableJoin;
import org.codefilarete.stalactite.engine.configurer.model.MappingJoin;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.King;
import org.codefilarete.stalactite.engine.model.Realm;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.Accessors.mutatorByMethodReference;
import static org.codefilarete.reflection.Accessors.readWriteAccessPoint;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.engine.configurer.resolver.PropertyMappingResolverTest.ABSTRACT_PROPERTY_MAPPING_COMPARATOR;
import static org.codefilarete.stalactite.engine.configurer.resolver.PropertyMappingResolverTest.ABSTRACT_PROPERTY_MAPPING_REPRESENTATION;
import static org.codefilarete.tool.collection.Iterables.first;
import static org.mockito.Mockito.mock;

class AggregateMetadataResolverTest {
	
	@Test
	void collect_oneEntity() {
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
				.mapKey(E::getPropE, IdentifierPolicy.databaseAutoIncrement())
				.map(E::getPropD);
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<E, Integer, ?> entity = testInstance.resolveEntityHierarchy(entityMappingBuilder.getConfiguration());
		assertThat(entity.getEntityType()).isEqualTo(E.class);
		assertThat(entity.getIdAccessor()).isEqualTo(readWriteAccessPoint(E::getPropE));
		assertThat(entity.getTable().getName()).isEqualTo("E");
		assertThat(entity.getPropertyMappingHolder().getWritablePropertyToColumn())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(D::getPropD), entity.getTable().getColumn("propD"), false, null, null, false)
				));
		assertThat(entity.getPropertyMappingHolder().getReadonlyPropertyToColumn()).isEmpty();
		assertThat(entity.getVersioning()).isNull();
	}
	
	@Test
	void collect_entityWithInheritance_entityAndEmbeddableInHierarchy_noJoiningTables() {
		FluentEntityMappingBuilder<E, Integer> entityMappingBuilder =
				entityBuilder(E.class, int.class)
						.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
						.map(E::getPropE)
						.mapSuperClass(embeddableBuilder(D.class)
								.map(D::getPropD)
								.map(B::getPropB)
								.map(A::getPropA)
						);
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<E, Integer, ?> entity = testInstance.resolveEntityHierarchy(entityMappingBuilder.getConfiguration());
		assertThat(entity.getEntityType()).isEqualTo(E.class);
		assertThat(entity.getIdAccessor()).isEqualTo(readWriteAccessPoint(C::getPropC));
		assertThat(entity.getTable().getName()).isEqualTo("E");
		assertThat(entity.getPropertyMappingHolder().getWritablePropertyToColumn())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(E::getPropE), entity.getTable().getColumn("propE"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(D::getPropD), entity.getTable().getColumn("propD"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(B::getPropB), entity.getTable().getColumn("propB"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(A::getPropA), entity.getTable().getColumn("propA"), false, null, null, false)
				));
		assertThat(entity.getPropertyMappingHolder().getReadonlyPropertyToColumn()).isEmpty();
		assertThat(entity.getVersioning()).isNull();
	}
	
	@Test
	void collect_entityWithInheritance_onlyEntityInHierarchy_noJoiningTables() {
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
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<E, Integer, ?> entity = testInstance.resolveEntityHierarchy(entityMappingBuilder.getConfiguration());
		
		assertThat(entity.getEntityType()).isEqualTo(E.class);
		assertThat(entity.getIdAccessor()).isEqualTo(readWriteAccessPoint(C::getPropC));
		assertThat(entity.getTable().getName()).isEqualTo("E");
		assertThat(entity.getPropertyMappingHolder().getWritablePropertyToColumn())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(E::getPropE), entity.getTable().getColumn("propE"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(D::getPropD), entity.getTable().getColumn("propD"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(B::getPropB), entity.getTable().getColumn("propB"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(A::getPropA), entity.getTable().getColumn("propA"), false, null, null, false)
				));
		assertThat(entity.getPropertyMappingHolder().getReadonlyPropertyToColumn()).isEmpty();
		assertThat(entity.getVersioning()).isNull();
	}	
	
	@Test
	<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	void collect_entityWithInheritance_onlyEntityInHierarchy_withJoiningTables() {
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
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<E, Integer, ?> entity = testInstance.resolveEntityHierarchy(entityMappingBuilder.getConfiguration());
		
		assertThat(entity.getEntityType()).isEqualTo(E.class);
		assertThat(entity.getIdAccessor()).isEqualTo(readWriteAccessPoint(C::getPropC));
		assertThat(entity.getIdentifierMapping()).isInstanceOf(AssignedByAnotherIdentifierMapping.class);
		assertThat(entity.getTable().getName()).isEqualTo("E");
		Column<LEFTTABLE, Integer> propE = ((LEFTTABLE) entity.getTable()).getColumn("propE");
		assertThat(entity.getPropertyMappingHolder().getWritablePropertyToColumn())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(E::getPropE), propE, false, null, null, false)
				));
		assertThat(entity.getPropertyMappingHolder().getReadonlyPropertyToColumn()).isEmpty();
		assertThat(entity.getVersioning()).isNull();
		
		AncestorJoin<? super E, LEFTTABLE, RIGHTTABLE, Integer> ancestor = (AncestorJoin<? super E, LEFTTABLE, RIGHTTABLE, Integer>) entity.getParent();
		assertThat(ancestor).isNotNull();
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, Integer> mergeJoin = ancestor.getJoin();
		Key<LEFTTABLE, ?> leftKey = mergeJoin.getLeftKey();
		assertThat(leftKey.getTable()).isSameAs(entity.getTable());
		Key<RIGHTTABLE, ?> rightKey = mergeJoin.getRightKey();
		Table<?> rightTable = rightKey.getTable();
		assertThat(rightTable.getName()).isEqualTo("D");
		// join is made on the propC columns
		assertThat(new KeepOrderSet<JoinLink<LEFTTABLE, ?>>(leftKey.getColumns())).containsExactly(leftKey.getTable().getColumn("propC"));
		assertThat(new KeepOrderSet<JoinLink<RIGHTTABLE, ?>>(rightKey.getColumns())).containsExactly(rightKey.getTable().getColumn("propC"));
		
		assertThat(ancestor.getAncestor().getIdentifierMapping()).isInstanceOf(SingleIdentifierMapping.class);
		assertThat(ancestor.getAncestor().getPropertyMappingHolder().getWritablePropertyToColumn())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(readWriteAccessPoint(D::getPropD), rightTable.getColumn("propD"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(B::getPropB), rightTable.getColumn("propB"), false, null, null, false),
						new PropertyMapping<>(readWriteAccessPoint(A::getPropA), rightTable.getColumn("propA"), false, null, null, false)
				));
		
		assertThat(ancestor.getAncestor().getParent()).isNull();
	}
	
	@Test
	<T extends Table<T>> void collect_entityWithInheritance_onlyEntityInHierarchy_withEmbeddable() {
		FluentEntityMappingBuilder<Realm, Integer> entityMappingBuilder = entityBuilder(Realm.class, int.class)
				.embed(Realm::getKing, embeddableBuilder(King.class)
						.map(King::getName).columnName("kingName")
				)
				.map(Country::setDescription).readonly()
				.mapSuperClass(entityBuilder(Country.class, int.class)
						.mapKey(Country::getVersion, IdentifierPolicy.databaseAutoIncrement()).columnName("myProperty")
						.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate).columnName("creation_date")
								.map(Timestamp::setModificationDate).setByConstructor()
						)
						.map(Country::setName).readonly());
		
		AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
		Entity<Realm, Integer, ?> actualResult = testInstance.resolveEntityHierarchy(entityMappingBuilder.getConfiguration());
		T countryTable = (T) actualResult.getTable();
		
		List<ReadWritePropertyAccessPoint<Country, Timestamp>> embeddablePrefix = Arrays.asList(readWriteAccessPoint(Country::getTimestamp));
		assertThat(actualResult.getPropertyMappingHolder().getWritablePropertyToColumn())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new PropertyMapping<>(new ReadWriteAccessorChain<>(Arrays.asList(readWriteAccessPoint(Realm::getKing)), readWriteAccessPoint(King::getName)), countryTable.getColumn("kingName"), false, null, null, false),
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::getCreationDate)), countryTable.getColumn("creation_date"), false, null, null, false),
						new PropertyMapping<>(new ReadWriteAccessorChain<>(embeddablePrefix, readWriteAccessPoint(Timestamp::setModificationDate)), countryTable.getColumn("modificationDate"), true, null, null, false)
				));
		
		assertThat(actualResult.getPropertyMappingHolder().getReadonlyPropertyToColumn())
				.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
				.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
				.isEqualTo(Arrays.asSet(
						new Entity.ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setDescription), countryTable.getColumn("description"), false, null, false),
						new Entity.ReadOnlyPropertyMapping<>(mutatorByMethodReference(Country::setName), countryTable.getColumn("name"), false, null, false)
				));
	}
	
	@Nested
	class ExtraTable {
		
		@Test
		void resolveEntityHierarchy_oneEntity() {
			Table extraTable1 = new Table("extraTable1");
			Table extraTable2 = new Table("extraTable2");
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder = entityBuilder(E.class, int.class)
					.mapKey(E::getPropE, IdentifierPolicy.databaseAutoIncrement())
					.map(E::getPropD)
					.map(E::getPropB).extraTable(extraTable1)
					.map(E::getPropC).extraTable(extraTable2);
			
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<E, Integer, ?> entity = testInstance.resolveEntityHierarchy(entityMappingBuilder.getConfiguration());
			
			
			assertThat(entity.getEntityType()).isEqualTo(E.class);
			assertThat(entity.getIdAccessor()).isEqualTo(readWriteAccessPoint(E::getPropE));
			assertThat(entity.getTable().getName()).isEqualTo("E");
			assertThat(entity.getPropertyMappingHolder().getWritablePropertyToColumn())
					.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
					.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
					.isEqualTo(Arrays.asSet(
							new PropertyMapping<>(readWriteAccessPoint(D::getPropD), entity.getTable().getColumn("propD"), false, null, null, false)
					));
			assertThat(entity.getPropertyMappingHolder().getReadonlyPropertyToColumn()).isEmpty();
			assertThat(entity.getVersioning()).isNull();
			assertThat(entity.getRelations()).hasSize(2);
			Column<?, Object> propEColumn = entity.getTable().getColumn("propE");
			assertThat(propEColumn.isPrimaryKey()).isTrue();
			
			Map<ReadWriteAccessPoint, MappingJoin<?, ?, ?>> map = Iterables.map(entity.getRelations(),
					relation -> ((PropertyMapping) first(((ExtraTableJoin) relation).getPropertyMappingHolder().getWritablePropertyToColumn())).getAccessPoint());
			ExtraTableJoin<?, ?, ?, ?> mergeJoin1 = (ExtraTableJoin<?, ?, ?, ?>) map.get(readWriteAccessPoint(B::getPropB));
			assertThat(mergeJoin1.getPropertyMappingHolder().getWritablePropertyToColumn())
					.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
					.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
					.isEqualTo(Arrays.asSet(
							new PropertyMapping<>(readWriteAccessPoint(B::getPropB), extraTable1.getColumn("propB"), false, null, null, false)
					));
			Set<JoinLink<?, ?>> leftColumns1 = new KeepOrderSet<>(mergeJoin1.getJoin().getLeftKey().getColumns());
			assertThat(leftColumns1).containsExactlyInAnyOrder(propEColumn);
			Set<JoinLink<?, ?>> rightColumns1 = new KeepOrderSet<>(mergeJoin1.getJoin().getRightKey().getColumns());
			assertThat(extraTable1.getColumn("propE").isPrimaryKey()).isTrue();
			assertThat(rightColumns1).containsExactly(extraTable1.getColumn("propE"));
			
			ExtraTableJoin<?, ?, ?, ?> mergeJoin2 = (ExtraTableJoin<?, ?, ?, ?>) map.get(readWriteAccessPoint(C::getPropC));
			assertThat(mergeJoin2.getPropertyMappingHolder().getWritablePropertyToColumn())
					.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
					.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
					.isEqualTo(Arrays.asSet(
							new PropertyMapping<>(readWriteAccessPoint(C::getPropC), extraTable2.getColumn("propC"), false, null, null, false)
					));
			Set<JoinLink<?, ?>> leftColumns2 = new KeepOrderSet<>(mergeJoin2.getJoin().getLeftKey().getColumns());
			assertThat(leftColumns2).containsExactlyInAnyOrder(propEColumn);
			Set<JoinLink<?, ?>> rightColumns2 = new KeepOrderSet<>(mergeJoin2.getJoin().getRightKey().getColumns());
			assertThat(extraTable2.getColumn("propE").isPrimaryKey()).isTrue();
			assertThat(rightColumns2).containsExactly(extraTable2.getColumn("propE"));
		}
		
		@Test
		<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		void resolveEntityHierarchy_entityWithInheritance_joiningTables() {
			Table extraTable1 = new Table("extraTable1");
			Table extraTable2 = new Table("extraTable2");
			
			FluentEntityMappingBuilder<E, Integer> entityMappingBuilder =
					entityBuilder(E.class, int.class)
							.map(E::getPropE).extraTable(extraTable1)
							.mapSuperClass(entityBuilder(D.class, int.class)
									.map(D::getPropD)
									.mapSuperClass(entityBuilder(C.class, int.class)
											.mapKey(C::getPropC, IdentifierPolicy.databaseAutoIncrement())
											.map(C::getPropB).extraTable(extraTable2)
											.mapSuperClass(entityBuilder(A.class, int.class)
													.map(A::getPropA)
											)
									)
							).joiningTables();
			
			AggregateMetadataResolver testInstance = new AggregateMetadataResolver(new DefaultDialect(), mock(ConnectionConfiguration.class));
			Entity<E, Integer, ?> entity = testInstance.resolveEntityHierarchy(entityMappingBuilder.getConfiguration());
			
			// Result:
			// - is an entity of class E
			// - targets table "E", that has a primary key on column "propC" (because C class defines it) 
			// - has no direct properties
			// - has one indirect property on table "extraTable1": propB
			assertThat(entity.getEntityType()).isEqualTo(E.class);
			assertThat(entity.getIdAccessor()).isEqualTo(readWriteAccessPoint(C::getPropC));
			assertThat(entity.getTable().getName()).isEqualTo("E");
			assertThat(entity.getPropertyMappingHolder().getWritablePropertyToColumn()).isEmpty();
			assertThat(entity.getPropertyMappingHolder().getReadonlyPropertyToColumn()).isEmpty();
			assertThat(entity.getVersioning()).isNull();
			assertThat(entity.getRelations()).hasSize(1);
			Column<?, Object> propCColumn = entity.getTable().getColumn("propC");
			assertThat(propCColumn.isPrimaryKey()).isTrue();
			
			// Result has one relation on table "extraTable1": propE
			Map<ReadWriteAccessPoint, MappingJoin<?, ?, ?>> relationsByAccessor = Iterables.map(entity.getRelations(),
					relation -> ((PropertyMapping) first(((ExtraTableJoin) relation).getPropertyMappingHolder().getWritablePropertyToColumn())).getAccessPoint());
			ExtraTableJoin<?, ?, ?, ?> mergeJoin1 = (ExtraTableJoin<?, ?, ?, ?>) relationsByAccessor.get(readWriteAccessPoint(E::getPropE));
			assertThat(mergeJoin1.getPropertyMappingHolder().getWritablePropertyToColumn())
					.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
					.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
					.isEqualTo(Arrays.asSet(
							new PropertyMapping<>(readWriteAccessPoint(E::getPropE), extraTable1.getColumn("propE"), false, null, null, false)
					));
			Set<JoinLink<?, ?>> leftColumns1 = new KeepOrderSet<>(mergeJoin1.getJoin().getLeftKey().getColumns());
			assertThat(leftColumns1).containsExactlyInAnyOrder(propCColumn);
			Set<JoinLink<?, ?>> rightColumns1 = new KeepOrderSet<>(mergeJoin1.getJoin().getRightKey().getColumns());
			assertThat(extraTable1.getColumn("propC").isPrimaryKey()).isTrue();
			assertThat(rightColumns1).containsExactly(extraTable1.getColumn("propC"));
			
			// Result parent:
			// - is an entity of class D
			// - targets table "D", that has a primary key on column "propC" (because C class defines it) 
			// - has 2 direct properties: propD and propA
			// - has one indirect property on table "extraTable2": propB
			AncestorJoin<? super E, LEFTTABLE, RIGHTTABLE, Integer> ancestor = (AncestorJoin<? super E, LEFTTABLE, RIGHTTABLE, Integer>) entity.getParent();
			assertThat(ancestor).isNotNull();
			DirectRelationJoin<LEFTTABLE, RIGHTTABLE, Integer> mergeJoin = ancestor.getJoin();
			Key<LEFTTABLE, ?> leftKey = mergeJoin.getLeftKey();
			assertThat(leftKey.getTable()).isSameAs(entity.getTable());
			Key<RIGHTTABLE, ?> rightKey = mergeJoin.getRightKey();
			Table<?> rightTable = rightKey.getTable();
			assertThat(rightTable.getName()).isEqualTo("D");
			// join is made on the propC columns
			assertThat(new KeepOrderSet<JoinLink<LEFTTABLE, ?>>(leftKey.getColumns())).containsExactly(leftKey.getTable().getColumn("propC"));
			assertThat(new KeepOrderSet<JoinLink<RIGHTTABLE, ?>>(rightKey.getColumns())).containsExactly(rightKey.getTable().getColumn("propC"));
			
			Entity<? super E, Integer, ?> ancestorEntity = ancestor.getAncestor();
			assertThat(ancestorEntity.getIdentifierMapping()).isInstanceOf(SingleIdentifierMapping.class);
			assertThat(ancestorEntity.getPropertyMappingHolder().getWritablePropertyToColumn())
					.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
					.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
					.isEqualTo(Arrays.asSet(
							new PropertyMapping<>(readWriteAccessPoint(D::getPropD), rightTable.getColumn("propD"), false, null, null, false),
							new PropertyMapping<>(readWriteAccessPoint(A::getPropA), rightTable.getColumn("propA"), false, null, null, false)
					));
			
			assertThat(ancestorEntity.getRelations()).hasSize(1);
			Column<?, Object> propEColumn = ancestorEntity.getTable().getColumn("propC");
			assertThat(propEColumn.isPrimaryKey()).isTrue();
			
			// Result parent has one indirect property on table "extraTable2": propB
			Map<ReadWriteAccessPoint, MappingJoin<?, ?, ?>> ancestorRelationsByAccessor = Iterables.map(ancestorEntity.getRelations(),
					relation -> ((PropertyMapping) first(((ExtraTableJoin) relation).getPropertyMappingHolder().getWritablePropertyToColumn())).getAccessPoint());
			ExtraTableJoin<?, ?, ?, ?> ancestorMergeJoin = (ExtraTableJoin<?, ?, ?, ?>) ancestorRelationsByAccessor.get(readWriteAccessPoint(B::getPropB));
			assertThat(ancestorMergeJoin.getPropertyMappingHolder().getWritablePropertyToColumn())
					.usingElementComparator(ABSTRACT_PROPERTY_MAPPING_COMPARATOR)
					.withRepresentation(ABSTRACT_PROPERTY_MAPPING_REPRESENTATION)
					.isEqualTo(Arrays.asSet(
							new PropertyMapping<>(readWriteAccessPoint(B::getPropB), extraTable2.getColumn("propB"), false, null, null, false)
					));
			Set<JoinLink<?, ?>> ancestorMergeJoinLeftColumns = new KeepOrderSet<>(ancestorMergeJoin.getJoin().getLeftKey().getColumns());
			assertThat(ancestorMergeJoinLeftColumns).containsExactlyInAnyOrder(propEColumn);
			Set<JoinLink<?, ?>> ancestorMergeRightColumns = new KeepOrderSet<>(ancestorMergeJoin.getJoin().getRightKey().getColumns());
			assertThat(extraTable2.getColumn("propC").isPrimaryKey()).isTrue();
			assertThat(ancestorMergeRightColumns).containsExactly(extraTable2.getColumn("propC"));
			
			assertThat(ancestorEntity.getParent()).isNull();
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
