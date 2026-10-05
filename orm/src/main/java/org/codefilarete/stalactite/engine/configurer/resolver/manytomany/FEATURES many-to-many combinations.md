Hereafter are the features that are supported by the resolver of many-to-many relations. This file also helps as a TODO list for current development.

| relation     | owner                       | fetching  | polymorphic target | status |
|--------------|-----------------------------|-----------|--------------------|--------|
| many-to-many | association table (default) | join      | no                 | [x]    |
| many-to-many | association table (default) | separate  | no                 | [x]    |
| many-to-many | target                      | join      | no                 | [x]    |
| many-to-many | target                      | separate  | no                 | [x]    |
| -----------  | --------------------------- | --------- | ------------------ | ------ |
| many-to-many | association table (default) | join      | yes                | [x]    |
| many-to-many | association table (default) | separate  | yes                | [x]    |
| many-to-many | target                      | join      | yes                | [x]    |
| many-to-many | target                      | separate  | yes                | [x]    |

# many-to-many
## with-association-table (default and only behavior)
An association table that owns the relation (the referencing columns are on the association table)
- [x] default use case :

  implementation :
    - write :
      - [ManyToManyResolver.buildAssociationTableEngine(..)](ManyToManyResolver.java#70) → [OneToManyWithAssociationTableEngine](../onetomany/OneToManyWithAssociationTableEngine.java)
      - [ManyToManyResolver.buildIndexedAssociationTableEngine(..)](ManyToManyResolver.java#169) → [OneToManyWithIndexedAssociationTableEngine](../onetomany/OneToManyWithIndexedAssociationTableEngine.java)
    - read :
      - [AggregateManyToManyAppender.append(..)](AggregateManyToManyAppender.java#79) → [AggregateJoinedManyToManyAppender.append(..)](AggregateJoinedManyToManyAppender.java#36)
      - [AggregateManyToManyAppender.append(..)](AggregateManyToManyAppender.java#73) → [AggregateJoinedManyToManyAppender.append(..)](AggregateJoinedManyToManyAppender.java#55)

  test : [ManyToManyResolverTest](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/manytomany/ManyToManyResolverTest.java)

- [x] with separate fetch (read) :

  implementation :
    - [AggregateManyToManyAppender.append(..)](AggregateManyToManyAppender.java#77) → [AggregateJoinedManyToManyAppender.append(..)](AggregateFetchSeparatelyManyToManyAppender.java#66)
    - [AggregateManyToManyAppender.append(..)](AggregateManyToManyAppender.java#71) → [AggregateFetchSeparatelyIndexedManyToManyAppender.append(..)](AggregateFetchSeparatelyIndexedManyToManyAppender.java#68)

  test :
    - [ManyToManyResolverTest.crud_insertAndSelect()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/manytomany/ManyToManyResolverTest.java#156)
    - [ManyToManyResolverTest.select_indexed_orderIsPreserved()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/manytomany/ManyToManyResolverTest.java#252)
    - [ManyToManyResolverTest.select_indexed_fetchSeparately_orderIsPreserved()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/manytomany/ManyToManyResolverTest.java#294)

# many-to-polymorphic-many
Support for many-to-many relations with a polymorphic target (right side of the relation)

## association-table (default and only behavior)
- [x] default use case :

  implementation :
  - [AggregateJoinedManyToManyAppender.append(..)](AggregateJoinedManyToManyAppender.java#78)
  - [AggregateJoinedIndexedManyToManyAppender.append(..)](AggregateJoinedIndexedManyToManyAppender.java#144)
    

  test :
  - table-per-class
      - [TablePerClassPolymorphismReaderTest.manyToTablePerClassMany()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#720)
      - [TablePerClassPolymorphismReaderTest.manyToTablePerClassMany_indexed()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#808)
  - single-table
     - [SingleTablePolymorphismReaderTest.manyToSingleTableMany()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#720)
     - [SingleTablePolymorphismReaderTest.manyToSingleTableMany_indexed()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#808)
  - join-table
    - [JoinTablePolymorphismReaderTest.manyToJoinTableMany()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#720)
    - [JoinTablePolymorphismReaderTest.manyToJoinSingleMany_indexed()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#808)

- [x] with separate fetch :

  implementation :
    - [AggregateFetchedSeparatelyManyToManyAppender.append(..)](AggregateFetchedSeparatelyManyToManyAppender.java#94)
    - [AggregateFetchedSeparatelyIndexedManyToManyAppender.append(..)](AggregateFetchedSeparatelyIndexedManyToManyAppender.java#100)

  test:
  - table-per-class
      - [TablePerClassPolymorphismReaderTest.manyToTablePerClassMany_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#900)
      - [TablePerClassPolymorphismReaderTest.manyToTablePerClassMany_indexed_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#989)
  - single-table
      - [SingleTablePolymorphismReaderTest.manyToSingleTableMany_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#900)
      - [SingleTablePolymorphismReaderTest.manyToSingleTableMany_indexed_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#989)
  - join-table
      - [JoinTablePolymorphismReaderTest.manyToJoinTableMany_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#900)
      - [JoinTablePolymorphismReaderTest.manyToJoinSingleMany_indexed_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#989)
