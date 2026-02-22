package org.codefilarete.stalactite.engine.runtime.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.configurer.DefaultComposedIdentifierAssembler;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecordMapping;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping.KeyValueRecordIdMapping;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
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
import org.codefilarete.stalactite.mapping.EmbeddedBeanMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.model.FluentQueries;
import org.codefilarete.stalactite.query.api.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.api.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.reflection.AccessorChain.fromMethodReference;
import static org.codefilarete.reflection.Accessors.accessor;
import static org.codefilarete.reflection.Accessors.accessorByMethodReference;
import static org.codefilarete.reflection.Accessors.mutatorByMethodReference;
import static org.codefilarete.tool.collection.Arrays.asList;
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
						.add(accessor(Person::getName), nameColumn)
		);
		when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
						.add(accessor(Person::getVersion), versionColumn)
		);
		
		IdMapping<Person, Identifier<Long>> idMapping = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
		when(idMapping.<T>getIdentifierAssembler()).thenReturn(new SingleIdentifierAssembler<>(idColumn));
		when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(accessor(Person::getId)));
		
		// When
		EntityJoinTree<Person, Identifier<Long>> personTree = new EntityJoinTree<>(entityMappingMock);
		AggregateAccessPointToColumnMapping<Person> testInstance = new AggregateAccessPointToColumnMapping<>(personTree, true);
		Map<List<? extends ValueAccessPoint<?>>, JoinLink<?, ?>> result = testInstance.getPropertyToColumn();
		
		// Then
		AccessorToColumnMap accessorToColumnMap = new AccessorToColumnMap();
		accessorToColumnMap.put(asList(fromMethodReference(Person::getId)), idColumn);
		accessorToColumnMap.put(asList(fromMethodReference(Person::getName)), nameColumn);
		accessorToColumnMap.put(asList(fromMethodReference(Person::getVersion)), versionColumn);
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
						.add(accessor(House::getSurname), nameColumn)
		);
		when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<House, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
						.add(accessor(House::getVersion), versionColumn)
		);
		
		IdMapping<House, House.HouseId> idMapping = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
		when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(accessor(House::getHouseId)));
		when(idMapping.<T>getIdentifierAssembler()).thenReturn(new DefaultComposedIdentifierAssembler<>(
				personTable,
				House.HouseId.class,
				Maps.forHashMap((Class<ReversibleAccessor<House.HouseId, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
						.add(accessor(House.HouseId::getNumber), numberColumn)
						.add(accessor(House.HouseId::getStreet), streetColumn)
		));
		
		// When
		EntityJoinTree<House, House.HouseId> personTree = new EntityJoinTree<>(entityMappingMock);
		AggregateAccessPointToColumnMapping<House> testInstance = new AggregateAccessPointToColumnMapping<>(personTree, true);
		Map<List<? extends ValueAccessPoint<?>>, JoinLink<?, ?>> result = testInstance.getPropertyToColumn();
		
		// Then
		AccessorToColumnMap accessorToColumnMap = new AccessorToColumnMap();
		accessorToColumnMap.put(asList(fromMethodReference(House::getHouseId), fromMethodReference(House.HouseId::getNumber)), numberColumn);
		accessorToColumnMap.put(asList(fromMethodReference(House::getHouseId), fromMethodReference(House.HouseId::getStreet)), streetColumn);
		accessorToColumnMap.put(asList(fromMethodReference(House::getSurname)), nameColumn);
		accessorToColumnMap.put(asList(fromMethodReference(House::getVersion)), versionColumn);
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
						.add(accessor(Person::getName), nameColumn)
		);
		when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T, ?>>) (Class) Column.class)
						.add(accessor(Person::getVersion), versionColumn)
		);
		
		IdMapping<Person, Identifier<Long>> idMapping = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
		when(idMapping.<T>getIdentifierAssembler()).thenReturn(new SingleIdentifierAssembler<>(idColumn));
		when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(accessor(Person::getId)));
		
		PseudoTable pseudoTable = new PseudoTable(FluentQueries.select(idColumn, nameColumn, versionColumn).from(personTable).getQuery(), "dummyUnion");
		ConfiguredRelationalPersister<Person, Identifier<Long>> rootPersisterMock = mock(ConfiguredRelationalPersister.class);
		when(rootPersisterMock.<T>getMapping()).thenReturn(entityMappingMock);
		
		// When
		EntityJoinTree<Person, Identifier<Long>> personTree = new EntityJoinTree<>(tree -> new TablePerClassRootJoinNode<>(tree,
				rootPersisterMock,
				Collections.emptyMap(),    // sub-persisters are not necessary for our test case
				pseudoTable,
				new SimpleSelectable<>("discriminatorColumn", String.class)));
		AggregateAccessPointToColumnMapping<Person> testInstance = new AggregateAccessPointToColumnMapping<>(personTree, true);
		Map<List<? extends ValueAccessPoint<?>>, JoinLink<?, ?>> result = testInstance.getPropertyToColumn();
		
		// Then
		AccessorToColumnMap accessorToColumnMap = new AccessorToColumnMap();
		// Note that discriminator is not present
		accessorToColumnMap.put(asList(fromMethodReference(Person::getId)), pseudoTable.findColumn(idColumn.getName()));
		accessorToColumnMap.put(asList(fromMethodReference(Person::getName)), pseudoTable.findColumn(nameColumn.getName()));
		accessorToColumnMap.put(asList(fromMethodReference(Person::getVersion)), pseudoTable.findColumn(versionColumn.getName()));
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
						.add(accessor(Person::getName), nameColumn)
		);
		when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T1, ?>>) (Class) Column.class)
						.add(accessor(Person::getVersion), versionColumn)
		);
		
		IdMapping<Person, Identifier<Long>> idMapping = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
		when(idMapping.<T1>getIdentifierAssembler()).thenReturn(new SingleIdentifierAssembler<>(idColumn));
		when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(accessor(Person::getId)));
		
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
		
		Accessor<Person, Set<String>> nicknamesAccessor = accessorByMethodReference(Person::getNicknames);
		BeanRelationFixer<Person, ElementRecord<String, Identifier<Long>>> relationFixer =
				BeanRelationFixer.ofAdapter(
						mutatorByMethodReference(Person::setNicknames)::set,
						accessorByMethodReference(Person::getNicknames)::get,
						HashSet::new,
						(bean, input, collection) -> collection.add(input.getElement()));
		
		// we add a relation node to declare the collection
		new RelationJoinNode<>(
				(JoinNode<?, T1>) personTree.getRoot(),
				(Accessor<Person, ElementRecord<String, Identifier<Long>>>) (Accessor) nicknamesAccessor,
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
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getId)), idColumn);
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getName)), nameColumn);
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getVersion)), versionColumn);
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getNicknames)), nicknameColumn);
		assertThat(testInstance.getPropertyToColumn()).isEqualTo(expectedAccessorToColumnMap);
	}
	
	@Test
	<T1 extends Table<T1>, T2 extends Table<T2>> void relationJoinNode_mapProperty_simpleKey_simpleValue() {
		// Given
		EntityMapping<Person, Identifier<Long>, T1> entityMappingMock = mock(EntityMapping.class);
		T1 personTable = (T1) new Table("Person");
		Column<T1, Identifier<Long>> idColumn = personTable.addColumn("id", Identifier.LONG_TYPE);
		Column<T1, String> nameColumn = personTable.addColumn("name", String.class);
		Column<T1, Long> versionColumn = personTable.addColumn("version", long.class);
		when(entityMappingMock.getTargetTable()).thenReturn(personTable);
		when(entityMappingMock.getPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T1, ?>>) (Class) Column.class)
						.add(accessor(Person::getName), nameColumn)
		);
		when(entityMappingMock.getReadonlyPropertyToColumn()).thenReturn(
				Maps.forHashMap((Class<ReversibleAccessor<Person, ?>>) (Class) ReversibleAccessor.class, (Class<Column<T1, ?>>) (Class) Column.class)
						.add(accessor(Person::getVersion), versionColumn)
		);
		
		IdMapping<Person, Identifier<Long>> idMapping = mock(IdMapping.class);
		when(entityMappingMock.getIdMapping()).thenReturn(idMapping);
		when(idMapping.<T1>getIdentifierAssembler()).thenReturn(new SingleIdentifierAssembler<>(idColumn));
		when(idMapping.getIdAccessor()).thenReturn(new AccessorWrapperIdAccessor<>(accessor(Person::getId)));
		
		EntityJoinTree<Person, Identifier<Long>> personTree = new EntityJoinTree<>(entityMappingMock);
		Key<T1, Identifier<Long>> leftJoinKey = Key.ofSingleColumn(idColumn);
		Set<Selectable<?>> selectables = Collections.emptySet();
		T2 mapTable = (T2) new Table<>("mapTable");
		Column<T2, Identifier<Long>> mapIdColumn = mapTable.addColumn("id", Identifier.LONG_TYPE).primaryKey();
		Column<T2, String> keyColumn = mapTable.addColumn("key", String.class);
		Column<T2, String> valueColumn = mapTable.addColumn("value", String.class);
		Key<T2, Identifier<Long>> rightJoinKey = Key.ofSingleColumn(mapIdColumn);
		PropertyAccessor<KeyValueRecord<Object, Object, Object>, Object> phoneNumbersAccessor = KeyValueRecord.VALUE_ACCESSOR;
		Map<ReversibleAccessor<?, ?>, Column> propertiesMapping = new HashMap<>();
		propertiesMapping.put(phoneNumbersAccessor, valueColumn);
		EntityMapping<KeyValueRecord<String, String, Identifier<Long>>, RecordId<String, Identifier<Long>>, T2> mapMapping = new KeyValueRecordMapping<>(
				mapTable,
				// Gross cast due to complex generics (and probably wrong in the code)
				(Map<? extends ReversibleAccessor<KeyValueRecord<String, String, Identifier<Long>>, ?>, Column<T2, ?>>) (Map) propertiesMapping,
				new KeyValueRecordIdMapping<>(mapTable, mock(EmbeddedBeanMapping.class), mock(IdentifierAssembler.class), null)
		);
		EntityInflater<KeyValueRecord<String, String, Identifier<Long>>, KeyValueRecord<String, String, Identifier<Long>>> entityInflater = mock(EntityInflater.class);
		// We must cast the collectionMapping variable to a lighter type because Mockito or the compiler seems to not be able to handle the
		// complexity of the generics type
		when(entityInflater.getEntityMapping()).thenReturn((EntityMapping) mapMapping);
		
		BeanRelationFixer<Person, KeyValueRecord<String, String, Identifier<Long>>> relationFixer =
				BeanRelationFixer.ofMapAdapter(
						mutatorByMethodReference(Person::setPhoneNumbers)::set,
						accessorByMethodReference(Person::getPhoneNumbers)::get,
						HashMap::new,
						(bean, input, collection) -> collection.put(input.getKey(), input.getValue()));
		
		// we add a relation node to declare the collection
		new RelationJoinNode<>(
				(JoinNode<?, T1>) personTree.getRoot(),
				accessorByMethodReference(Person::getPhoneNumbers),
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
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getId)), idColumn);
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getName)), nameColumn);
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getVersion)), versionColumn);
		expectedAccessorToColumnMap.put(asList(accessorByMethodReference(Person::getPhoneNumbers)), valueColumn);
		assertThat(testInstance.getPropertyToColumn()).isEqualTo(expectedAccessorToColumnMap);
	}
}
