package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.engine.runtime.EntityCriteriaSupport.AggregateAccessPointToColumnMapping;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.runtime.EntityCriteriaSupport.AccessorToColumnMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityCriteriaSupportTest {
	
	@Test
	void apiUsage() {
		
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		RelationalEntityPersister<Country, Identifier<Long>> persister = (RelationalEntityPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::getName)
				.mapOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName))
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		// Note that this constructor can't accept relation as criteria, else we should register relations on it
		// which can be quite difficult because we don't have access to internal objects behind persister variable 
		EntityCriteria<Country, ?> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getEntityJoinTree(), true)
				.and(Country::getId, Operators.in("11"))
				.and(Country::getName, Operators.eq("toto"))
				.and(Country::getName, Operators.between("11", ""))
				.and(Country::getName, Operators.gteq("11"))
				.and(Country::setName, Operators.in("11"))
				.and(Country::setName, Operators.between("11", ""))
				.and(Country::setName, Operators.gteq("11"))
				.or(Country::getId, Operators.in("11"))
				.or(Country::getName, Operators.eq("toto"))
				.or(Country::getName, Operators.between("11", ""))
				.or(Country::getName, Operators.gteq("11"))
				.or(Country::setName, Operators.in("11"))
				.or(Country::setName, Operators.between("11", ""))
				.or(Country::setName, Operators.gteq("11"))
				;
		
		persister.selectWhere(Country::getCapital, City::getId, Operators.gteq("11"))
				.and(Country::getCapital, City::getId, Operators.gteq("11"));
	}
	
	@Test
	void hasCollectionCriteria_noCollectionCriteria_returnsFalse() {
		Table personTable = new Table("Person");
		Column nameColumn = personTable.addColumn("name", String.class);
		
		EntityMapping entityMappingMock = mock(EntityMapping.class);
		when(entityMappingMock.getPropertyToColumn()).thenReturn(Maps.forHashMap(ReversibleAccessor.class, Column.class).add(Accessors.accessor(Person::getName), nameColumn));
		// we have to mock the identifier mapping because its columns are collected as eventual criteria (else a NPE is thrown)
		when(entityMappingMock.getIdMapping()).thenReturn(mock(IdMapping.class));
		when(entityMappingMock.getTargetTable()).thenReturn(personTable);
		EntityCriteriaSupport<Person> testInstance = new EntityCriteriaSupport<>(new EntityJoinTree<Person, Object>(entityMappingMock), true);
		// first check: when there's no criteria, there's also no collection criteria !
		assertThat(testInstance.hasCollectionCriteria()).isFalse();
		
		testInstance.and(Person::getName, Operators.eq(""));
		assertThat(testInstance.hasCollectionCriteria()).isFalse();
	}
	
	@Test
	void hasCollectionCriteria_embeddedCollectionCriteria_returnsTrue() {
		Table personTable = new Table("Person");
		Column nameColumn = personTable.addColumn("name", String.class);
		
		EntityMapping entityMappingMock = mock(EntityMapping.class);
		when(entityMappingMock.getPropertyToColumn()).thenReturn(Maps.forHashMap(ReversibleAccessor.class, Column.class).add(Accessors.accessor(Person::getNicknames), nameColumn));
		// we have to mock the identifier mapping because its columns are collected as eventual criteria (else a NPE is thrown)
		when(entityMappingMock.getIdMapping()).thenReturn(mock(IdMapping.class));
		when(entityMappingMock.getTargetTable()).thenReturn(personTable);
		EntityCriteriaSupport<Person> testInstance = new EntityCriteriaSupport<Person>(new EntityJoinTree<Person, Object>(entityMappingMock), true) {
			@Override
			void appendAsCriterion(LogicalOperator logicalOperator, List accessPointChain, ConditionalOperator operator) {
				// overridden to do nothing because it's too complex and that's not the goal of our test method
			}
		};
		testInstance.and(Person::getNicknames, Operators.eq(""));
		assertThat(testInstance.hasCollectionCriteria()).isTrue();
	}
	
	@Nested
	class CollectPropertyMapping {
		
		@Test
		<T extends Table<T>> void rootNode_withSimpleIdentifierMapping() {
			// Given
			EntityMapping<Person, Identifier<Long>, T> entityMappingMock = mock(EntityMapping.class);
			T personTable = (T) new Table("Person");
			Column<T, Identifier<Long>> idColumn = personTable.addColumn("id", Identifier.LONG_TYPE);
			Column<T, String> nameColumn = personTable.addColumn("name", String.class);
			Column<T, Long> versionColumn = personTable.addColumn("version", long.class);
			when(entityMappingMock.getTargetTable()).thenReturn(personTable);
			when(entityMappingMock.getPropertyToColumn()).thenReturn(
					Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
							.add(Accessors.accessor(Person::getName), nameColumn)
			);
			when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
					Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
							.add(Accessors.accessor(Person::getVersion), versionColumn)
			);
			
			IdMapping<Person, Identifier<Long>> idMapping = mock(IdMapping.class);
			when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
			when(idMapping.<T>getIdentifierAssembler()).thenReturn(new SingleIdentifierAssembler<>(idColumn));
			when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(Accessors.accessor(Person::getId)));
			
			// When
			EntityJoinTree<Person, Identifier<Long>> personTree = new EntityJoinTree<>(entityMappingMock);
			AggregateAccessPointToColumnMapping<Person> testInstance = new AggregateAccessPointToColumnMapping<>(personTree, true);
			Map<List<ValueAccessPoint<?>>, Selectable<?>> result = testInstance.collectPropertyMapping(personTree.getRoot(), new ArrayDeque<>());
			
			// Then
			AccessorToColumnMap accessorToColumnMap = new AccessorToColumnMap();
			accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(Person::getId)), idColumn);
			accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(Person::getName)), nameColumn);
			accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(Person::getVersion)), versionColumn);
			assertThat(result).isEqualTo(accessorToColumnMap);
		}

		@Test
		<T extends Table<T>> void rootNode_withComplexIdentifierMapping() {
			// Given
			EntityMapping<House, House.HouseId, T> entityMappingMock = mock(EntityMapping.class);
			T personTable = (T) new Table("House");
			Column<T, House.HouseId> idColumn = personTable.addColumn("id", House.HouseId.class);
			Column<T, String> nameColumn = personTable.addColumn("name", String.class);
			Column<T, Long> versionColumn = personTable.addColumn("version", long.class);
			Column<T, Integer> numberColumn = personTable.addColumn("number", int.class);
			Column<T, String> streetColumn = personTable.addColumn("street", String.class);
			when(entityMappingMock.getTargetTable()).thenReturn(personTable);
			when(entityMappingMock.getPropertyToColumn()).thenReturn(
					Maps.forHashMap((Class<ReversibleAccessor<House, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
							.add(Accessors.accessor(House::getSurname), nameColumn)
			);
			when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
					Maps.forHashMap((Class<ReversibleAccessor<House, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
							.add(Accessors.accessor(House::getVersion), versionColumn)
			);

			IdMapping<House, House.HouseId> idMapping = mock(IdMapping.class);
			when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
			when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(Accessors.accessor(House::getHouseId)));
			when(idMapping.<T>getIdentifierAssembler()).thenReturn(new DefaultComposedIdentifierAssembler<>(
					personTable,
					House.HouseId.class,
					Maps.forHashMap((Class<ReversibleAccessor<House.HouseId, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
							.add(Accessors.accessor(House.HouseId::getNumber), numberColumn)
							.add(Accessors.accessor(House.HouseId::getStreet), streetColumn)
			));

			// When
			EntityJoinTree<House, House.HouseId> personTree = new EntityJoinTree<>(entityMappingMock);
			AggregateAccessPointToColumnMapping<House> testInstance = new AggregateAccessPointToColumnMapping<>(personTree, true);
			Map<List<ValueAccessPoint<?>>, Selectable<?>> result = testInstance.collectPropertyMapping(personTree.getRoot(), new ArrayDeque<>());

			// Then
			AccessorToColumnMap accessorToColumnMap = new AccessorToColumnMap();
			accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getHouseId), AccessorChain.fromMethodReference(House.HouseId::getNumber)), numberColumn);
			accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getHouseId), AccessorChain.fromMethodReference(House.HouseId::getStreet)), streetColumn);
			accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getSurname)), nameColumn);
			accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getVersion)), versionColumn);
			assertThat(result).isEqualTo(accessorToColumnMap);
		}
	}
}