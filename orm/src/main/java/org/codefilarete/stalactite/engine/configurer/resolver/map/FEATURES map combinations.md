Hereafter are the features that are supported by the resolver of map relations. This file also helps as a TODO list for current development.

| relation | key as entity    | value as entity | fetching  | polymorphic target | status |
|----------|------------------|-----------------|-----------|--------------------|--------|
| map      | yes              | no              | join      | no                 | [x]    |
| map      | yes              | no              | join      | yes                | [x]    |
| map      | yes              | no              | separate  | no                 | [x]    |
| map      | yes              | no              | separate  | yes                | [x]    |
| -------- | ---------------- | --------------- | --------- | ------------------ | ------ |
| map      | no               | yes             | join      | no                 | [x]    |
| map      | no               | yes             | join      | yes                | [x]    |
| map      | no               | yes             | separate  | no                 | [x]    |
| map      | no               | yes             | separate  | yes                | [x]    |
| -------- | ---------------- | --------------- | --------- | ------------------ | ------ |
| map      | yes              | yes             | join      | no                 | [x]    | MapResolverTest.crud_keyAndValueAreEntities
| map      | yes              | yes             | join      | yes                | [x]    | *PolymorphismReaderTest.select_keyAndValueAreEntities
| map      | yes              | yes             | separate  | no                 | [x]    | MapResolverTest.select_keyAndValueAreEntities_bothFetchSeparately
| map      | yes              | yes             | separate  | yes                | [x]    | *PolymorphismReaderTest.select_keyAndValueAreEntities_bothFetchSeparately
| -------- | ---------------- | --------------- | --------- | ------------------ | ------ |
| map      | no               | no              | join      |                    | [x]    |


# map
Note that key and value as entity share some code to avoid redundancy. For example, see [JoinedMapAppender](JoinedMapAppender.java) and [FetchedSeparatelyMapAppender](FetchedSeparatelyMapAppender.java) which are used for both key and value as entity.

## key is an entity
Support for map relations with entity as key
- [x] default use case :

  implementation :
    - write : [MapResolver.resolve(..)](MapResolver.java#33) → [EntryMapResolver](EntryMapResolver.java) → [MapUpdater](MapUpdater.java)
    - read : [AggregateMapAppender.append(..)](AggregateMapAppender.java#28) → [JoinedMapAppender](JoinedMapAppender.java#51)

  test : [MapResolverTest.EntityAsMapKey.crud_entityAsMapKey()](MapResolverTest.java#638)

- [x] with separate fetch (read) :

  implementation : [FetchedSeparatelyMapAppender.append(..)](FetchedSeparatelyMapAppender.java#58)
  test : [MapResolverTest.EntityAsMapKey.select_entityAsMapKey_fetchSeparately()](MapResolverTest.java#684)

## value is an entity
Support for map relations with entity as value
- [x] default use case :

  implementation :
    - write : [MapResolver.resolve(..)](MapResolver.java#33) → [EntryMapResolver](EntryMapResolver.java) → [MapUpdater](MapUpdater.java)
    - read : [AggregateMapAppender.append(..)](AggregateMapAppender.java#28) → [JoinedMapAppender](JoinedMapAppender.java#51)

  test : [MapResolverTest.EntityAsMapValue.crud_entityAsMapValue()](MapResolverTest.java#773)

- [x] with separate fetch (read) :

  implementation : [FetchedSeparatelyMapAppender.append(..)](FetchedSeparatelyMapAppender.java#58)
  test : [MapResolverTest.EntityAsMapValue.select_entityAsMapValue_fetchSeparately()](MapResolverTest.java#819)

# map-with-polymorphic-key
Support for map relations with polymorphic entity as key

- [x] default use case :

  implementation : [JoinedMapAppender](JoinedMapAppender.java#51)

  test :
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_keysAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#1365)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_keysAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#1365)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_keysAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#1365)

- [x] with separate fetch :

  implementation : [FetchedSeparatelyMapAppender.append(..)](FetchedSeparatelyMapAppender.java#58), [FetchedSeparatelyMapAppender.appendSeparatelyFetchedEntityJoin(..)](FetchedSeparatelyMapAppender.java#156)

  test:
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_keysAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#1416)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_keysAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#1416)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_keysAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#1416)

# map-with-polymorphic-value
Support for map relations with polymorphic entity as value

- [x] default use case :

  implementation : [JoinedMapAppender](JoinedMapAppender.java#51)

  test :
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_valuesAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#1262)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_valuesAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#1262)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_valuesAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#1262)

- [x] with separate fetch :

  implementation : [FetchedSeparatelyMapAppender.append(..)](FetchedSeparatelyMapAppender.java#58), [FetchedSeparatelyMapAppender.appendSeparatelyFetchedEntityJoin(..)](FetchedSeparatelyMapAppender.java#156)

  test:
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_valuesAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#1313)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_valuesAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#1313)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_valuesAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#1313)
