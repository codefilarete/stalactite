Hereafter are the features that are supported by the resolver of map relations. This file also helps as a TODO list for current development.

| relation | key as entity    | value as entity | fetching  | polymorphic target | status |
|----------|------------------|-----------------|-----------|--------------------|--------|
| map      | yes              | no              | join      | no                 | [ ]    |
| map      | yes              | no              | join      | yes                | [ ]    |
| map      | yes              | no              | separate  | no                 | [ ]    |
| map      | yes              | no              | separate  | yes                | [ ]    |
| -------- | ---------------- | --------------- | --------- | ------------------ | ------ |
| map      | no               | yes             | join      | no                 | [x]    |
| map      | no               | yes             | join      | yes                | [x]    |
| map      | no               | yes             | separate  | no                 | [x]    |
| map      | no               | yes             | separate  | yes                | [x]    |
| -------- | ---------------- | --------------- | --------- | ------------------ | ------ |
| map      | yes              | yes             | join      | no                 | [ ]    |
| map      | yes              | yes             | join      | yes                | [ ]    |
| map      | yes              | yes             | separate  | no                 | [ ]    |
| map      | yes              | yes             | separate  | yes                | [ ]    |
| -------- | ---------------- | --------------- | --------- | ------------------ | ------ |
| map      | no               | no              | join      |                    | [x]    |


# map
Note that key and value as entity share some code to avoid redundancy. For example, see [JoinedMapAppender](JoinedMapAppender.java) and [FetchedSeparatelyMapAppender](FetchedSeparatelyMapAppender.java) which are used for both key and value as entity.

## key is an entity
Support for map relations with entity as key
- [ ] default use case :

  implementation :
    - write : [MapResolver.resolve(..)](MapResolver.java#33) → [EntryMapResolver](EntryMapResolver.java) → [MapUpdater](MapUpdater.java)
    - read : [AggregateMapAppender.append(..)](AggregateMapAppender.java#28) → [JoinedMapAppender](JoinedMapAppender.java#51)

  test : [MapResolverTest.EntityAsMapKey.crud_entityAsMapKey()](MapResolverTest.java#638)

- [ ] with separate fetch (read) :

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

- [ ] default use case :

  implementation : [JoinedMapAppender](JoinedMapAppender.java#51)

  test :
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_keyAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#905)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_keyAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#905)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_keyAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#905)

- [ ] with separate fetch :

  implementation : [FetchedSeparatelyMapAppender.append(..)](FetchedSeparatelyMapAppender.java#58), [FetchedSeparatelyMapAppender.appendSeparatelyFetchedEntityJoin(..)](FetchedSeparatelyMapAppender.java#156)

  test:
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_keyAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#990)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_keyAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#990)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_keyAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#990)

# map-with-polymorphic-value
Support for map relations with polymorphic entity as value

- [x] default use case :

  implementation : [JoinedMapAppender](JoinedMapAppender.java#51)

  test :
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_valueAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#905)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_valueAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#905)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_valueAreEntities()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#905)

- [x] with separate fetch :

  implementation : [FetchedSeparatelyMapAppender.append(..)](FetchedSeparatelyMapAppender.java#58), [FetchedSeparatelyMapAppender.appendSeparatelyFetchedEntityJoin(..)](FetchedSeparatelyMapAppender.java#156)

  test:
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.MapWithEntities.crud_valueAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#990)
    - single-table
        - [SingleTablePolymorphismReaderTest.MapWithEntities.crud_valueAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#990)
    - join-table
        - [JoinTablePolymorphismReaderTest.MapWithEntities.crud_valueAreEntities_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#990)
