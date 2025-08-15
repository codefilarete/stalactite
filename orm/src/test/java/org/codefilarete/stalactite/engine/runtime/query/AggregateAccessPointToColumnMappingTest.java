package org.codefilarete.stalactite.engine.runtime.query;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecordMapping;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport.AccessorToColumnMap;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AggregateAccessPointToColumnMappingTest {
	
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
		Map<List<? extends ValueAccessPoint<?>>, Selectable<?>> result = testInstance.getPropertyToColumn();
		
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
		Map<List<? extends ValueAccessPoint<?>>, Selectable<?>> result = testInstance.getPropertyToColumn();
		
		// Then
		AccessorToColumnMap accessorToColumnMap = new AccessorToColumnMap();
		accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getHouseId), AccessorChain.fromMethodReference(House.HouseId::getNumber)), numberColumn);
		accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getHouseId), AccessorChain.fromMethodReference(House.HouseId::getStreet)), streetColumn);
		accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getSurname)), nameColumn);
		accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(House::getVersion)), versionColumn);
		assertThat(result).isEqualTo(accessorToColumnMap);
	}
	
	@Test
	<T extends Table<T>> void tablePerClassRootJoinNode_withSimpleIdentifierMapping() {
		// Given
		EntityMapping<Person, Identifier<Long>, T> entityMappingMock = mock(EntityMapping.class);
		T personTable = (T) new Table("Person");
		Column<T, Identifier<Long>> idColumn = personTable.addColumn("id", Identifier.LONG_TYPE);
		Column<T, String> nameColumn = personTable.addColumn("name", String.class);
		Column<T, Long> versionColumn = personTable.addColumn("version", long.class);
		Column<T, Long> discriminatorColumn = personTable.addColumn("DTYPE", long.class);
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
		
		PseudoTable pseudoTable = new PseudoTable(QueryEase.select(idColumn, nameColumn, versionColumn).from(personTable).getQuery(), "dummyUnion");
		ConfiguredRelationalPersister<Person, Identifier<Long>> rootPersisterMock = mock(ConfiguredRelationalPersister.class);
		when(rootPersisterMock.<T>getMapping()).thenReturn(entityMappingMock);
		
		// When
		EntityJoinTree<Person, Identifier<Long>> personTree = new EntityJoinTree<>(tree -> new TablePerClassRootJoinNode<>(tree,
				rootPersisterMock,
				Collections.emptyMap(),    // sub-persisters are not necessary for our test case
				pseudoTable,
				new SimpleSelectable<>("discriminatorColumn", String.class)));
		AggregateAccessPointToColumnMapping<Person> testInstance = new AggregateAccessPointToColumnMapping<>(personTree, true);
		Map<List<? extends ValueAccessPoint<?>>, Selectable<?>> result = testInstance.getPropertyToColumn();
		
		// Then
		AccessorToColumnMap accessorToColumnMap = new AccessorToColumnMap();
		// Note that discriminator is not present
		accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(Person::getId)), pseudoTable.findColumn(idColumn.getName()));
		accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(Person::getName)), pseudoTable.findColumn(nameColumn.getName()));
		accessorToColumnMap.put(Arrays.asList(AccessorChain.fromMethodReference(Person::getVersion)), pseudoTable.findColumn(versionColumn.getName()));
		assertThat(result).isEqualTo(accessorToColumnMap);
	}
	
	@Test
	<T1 extends Table<T1>, T2 extends Table<T2>> void relationJoinNode_collectionProperty() {
		// Given
		EntityMapping<Person, Identifier<Long>, T1> entityMappingMock = mock(EntityMapping.class);
		T1 personTable = (T1) new Table("Person");
		Column<T1, Identifier<Long>> idColumn = personTable.addColumn("id", Identifier.LONG_TYPE);
		Column<T1, String> nameColumn = personTable.addColumn("name", String.class);
		Column<T1, Long> versionColumn = personTable.addColumn("version", long.class);
		when(entityMappingMock.getTargetTable()).thenReturn(personTable);
		when(entityMappingMock.getPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T1, ?>>) (Class) Column.class)
						.add(Accessors.accessor(Person::getName), nameColumn)
		);
		when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T1, ?>>) (Class) Column.class)
						.add(Accessors.accessor(Person::getVersion), versionColumn)
		);
		
		IdMapping<Person, Identifier<Long>> idMapping = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
		when(idMapping.<T1>getIdentifierAssembler()).thenReturn(new SingleIdentifierAssembler<>(idColumn));
		when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(Accessors.accessor(Person::getId)));
		
		EntityJoinTree<Person, Identifier<Long>> personTree = new EntityJoinTree<>(entityMappingMock);
		Key<T1, Identifier<Long>> leftJoinKey = Key.ofSingleColumn(idColumn);
		Set<Selectable<?>> selectables = Collections.emptySet();
		T2 collectionTable = (T2) new Table<>("collectionTable");
		Column<T2, Identifier<Long>> collectionIdColumn = collectionTable.addColumn("id", Identifier.LONG_TYPE).primaryKey();
		Column<T2, String> nicknameColumn = collectionTable.addColumn("nickname", String.class);
		Key<T2, Identifier<Long>> rightJoinKey = Key.ofSingleColumn(collectionIdColumn);
		EntityMapping<ElementRecord<String, Identifier<Long>>, ElementRecord<String, Identifier<Long>>, ?> collectionMapping = new ElementRecordMapping<>(
				collectionTable,
				nicknameColumn,
				(IdentifierAssembler<Identifier<Long>, T1>) mock(IdentifierAssembler.class),
				Collections.emptyMap()
		);
		EntityInflater<ElementRecord<String, Identifier<Long>>, ElementRecord<String, Identifier<Long>>> entityInflater = mock(EntityInflater.class);
		// We must cast the collectionMapping variable to a lighter type because Mockito or the compiler seems to not be able to handle the
		// complexity of the generics type
		when(entityInflater.getEntityMapping()).thenReturn((EntityMapping) collectionMapping);
		
		Accessor<Person, Set<String>> personSetAccessorByMethodReference = Accessors.accessorByMethodReference(Person::getNicknames);
		BeanRelationFixer<Person, ElementRecord<String, Identifier<Long>>> relationFixer =
				BeanRelationFixer.ofAdapter(
						Accessors.mutatorByMethodReference(Person::setNicknames)::set,
						Accessors.accessorByMethodReference(Person::getNicknames)::get,
						HashSet::new,
						(bean, input, collection) -> collection.add(input.getElement()));
		
		RelationJoinNode<ElementRecord<String, Identifier<Long>>, T1, T2, Identifier<Long>, ElementRecord<String, Identifier<Long>>> relationJoinNode = new RelationJoinNode<>(
				(JoinNode<?, T1>) personTree.getRoot(),
				(Accessor<Person, ElementRecord<String, Identifier<Long>>>) (Accessor) personSetAccessorByMethodReference,
				leftJoinKey,
				rightJoinKey,
				JoinType.OUTER,
				selectables,
				"tableAlias",
				entityInflater,
				relationFixer,
				null);
		
		// When
		AggregateAccessPointToColumnMapping<Person> testInstance = new AggregateAccessPointToColumnMapping<>(personTree, true);
		
		// Then
		AccessorToColumnMap expectedAccessorToColumnMap = new AccessorToColumnMap();
		expectedAccessorToColumnMap.put(Arrays.asList(Accessors.accessorByMethodReference(Person::getId)), idColumn);
		expectedAccessorToColumnMap.put(Arrays.asList(Accessors.accessorByMethodReference(Person::getName)), nameColumn);
		expectedAccessorToColumnMap.put(Arrays.asList(Accessors.accessorByMethodReference(Person::getVersion)), versionColumn);
		expectedAccessorToColumnMap.put(Arrays.asList(Accessors.accessorByMethodReference(Person::getNicknames)), nicknameColumn);
		assertThat(testInstance.getPropertyToColumn()).isEqualTo(expectedAccessorToColumnMap);
	}
}