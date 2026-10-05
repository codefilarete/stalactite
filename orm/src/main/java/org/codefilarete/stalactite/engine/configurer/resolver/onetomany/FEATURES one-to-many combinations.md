Hereafter are the features that are supported by the resolver of one-to-many relations. This file also helps as a TODO list for current development.

| relation    | owner                       | fetching  | polymorphic target | status |
|-------------|-----------------------------|-----------|--------------------|--------|
| one-to-many | association table (default) | join      | no                 | [x]    |
| one-to-many | association table (default) | separate  | no                 | [x]    |
| one-to-many | target                      | join      | no                 | [x]    |
| one-to-many | target                      | separate  | no                 | [x]    |
| ----------- | --------------------------- | --------- | ------------------ | ------ |
| one-to-many | association table (default) | join      | yes                | [x]    |
| one-to-many | association table (default) | separate  | yes                | [x]    |
| one-to-many | target                      | join      | yes                | [x]    |
| one-to-many | target                      | separate  | yes                | [x]    |



# one-to-many
## with-association-table (default behavior)
An association table that owns the relation (the referencing columns are on the association table)
- [x] default use case :

  implementation :
    - write :
      - [OneToManyResolver.buildAssociationTableEngine(..)](../         OneToManyResolver.java#137) → [OneToManyWithAssociationTableEngine](../         OneToManyWithAssociationTableEngine.java)
      - [OneToManyResolver.buildIndexedAssociationTableEngine(..)](../         OneToManyResolver.java#169) → [OneToManyWithIndexedAssociationTableEngine](../         OneToManyWithIndexedAssociationTableEngine.java)
    - read :
      - [AggregateOneToManyAppender.append(..)](../         AggregateOneToManyAppender.java#45) → [AggregateOneToManyWithAssociationTableAppender.append(..)](../         AggregateOneToManyWithAssociationTableAppender.java#62)
      - [AggregateOneToManyAppender.append(..)](../         AggregateOneToManyAppender.java#45) → [AggregateOneToManyWithIndexedAssociationTableAppender.append(..)](../AggregateOneToManyWithIndexedAssociationTableAppender.java#72)

  test : [OneToManyResolverTest.AssociationTable](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/OneToManyResolverTest.java#483)

- [x] with separate fetch (read) :

  implementation : [AggregateOneToOneAppender.append(..)](../onetoone/AggregateOneToOneAppender.java#63)

  test :
    - [OneToManyResolverTest.AssociationTable.select_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/OneToManyResolverTest.java#649)
    - [OneToManyResolverTest.AssociationTable.select_indexed_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/OneToManyResolverTest.java#679)
    - [OneToManyResolverTest.AssociationTable.select_indexed()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/OneToManyResolverTest.java#715)

## owned-by-target
The target entity is the owner of the relation (the referencing column is on the target table)
- [x] default use case :

  implementation :
    - write : [OneToManyResolver.resolve(..)](OneToManyResolver.java#63)
        - [OneToManyWithMappedAssociationEngine](OneToManyWithMappedAssociationEngine.java#36)
        - [OneToManyWithIndexedMappedAssociationEngine](OneToManyWithIndexedMappedAssociationEngine.java#34)
    - read : [AggregateOneToManyAppender.append(..)](AggregateOneToManyAppender.java#28)

  test : [OneToManyResolverTest.MappedBy.select_indexed](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/onetomany/OneToManyResolverTest.java#388)

- [x] with separate fetch (read) :

  implementation :
    - [AggregateOneToManyWithMappedAssociationAppender.append(..)](AggregateOneToManyWithMappedAssociationAppender.java#128)
    - [AggregateOneToManyWithIndexedMappedAssociationAppender.append(..)](AggregateOneToManyWithIndexedMappedAssociationAppender.java#121)

  test : [OneToManyResolverTest.MappedBy](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/configurer/resolver/onetomany/OneToManyResolverTest.java#223)

# one-to-polymorphic-many
Support for one-to-many relations with a polymorphic target (right side of the relation)

## association-table (default behavior)
- [x] default use case :

  implementation :
  - [AggregateOneToManyWithAssociationTableAppender.append(..)](AggregateOneToManyWithAssociationTableAppender.java#78)
  - [AggregateOneToManyWithIndexedAssociationTableAppender.append(..)](AggregateOneToManyWithIndexedAssociationTableAppender.java#144)
    

  test :
  - table-per-class
      - [TablePerClassPolymorphismReaderTest.oneToTablePerClassMany()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#357)
      - [TablePerClassPolymorphismReaderTest.oneToTablePerClassMany_indexed()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#490)
  - single-table
     - [SingleTablePolymorphismReaderTest.oneToSingleTableMany()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#357)
     - [SingleTablePolymorphismReaderTest.oneToSingleTableMany_indexed()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#490)
  - join-table
    - [JoinTablePolymorphismReaderTest.oneToJoinTableMany()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#357)
    - [JoinTablePolymorphismReaderTest.oneToJoinSingleMany_indexed()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#490)

- [x] with separate fetch :

  implementation :
    - [AggregateOneToManyWithAssociationTableAppender.appendWithSeparateFetch(..)](AggregateOneToManyWithAssociationTableAppender.java#158)
    - [AggregateOneToManyWithIndexedAssociationTableAppender.appendWithSeparateFetch(..)](AggregateOneToManyWithIndexedAssociationTableAppender.java#234)

  test:
  - table-per-class
      - [TablePerClassPolymorphismReaderTest.oneToTablePerClassMany_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#423)
      - [TablePerClassPolymorphismReaderTest.oneToTablePerClassMany_indexed_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#559)
  - single-table
      - [SingleTablePolymorphismReaderTest.oneToSingleTableMany_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#423)
      - [SingleTablePolymorphismReaderTest.oneToSingleTableMany_indexed_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#559)
  - join-table
      - [JoinTablePolymorphismReaderTest.oneToJoinTableMany_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#423)
      - [JoinTablePolymorphismReaderTest.oneToJoinSingleMany_indexed_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#559)

## owned-by-target
- [x] default use case :

  implementation :
    - [AggregateOneToManyWithMappedAssociationAppender.append(..)](AggregateOneToManyWithMappedAssociationAppender.java#43)
    - [AggregateOneToManyWithIndexedMappedAssociationAppender.append(..)](AggregateOneToManyWithIndexedMappedAssociationAppender.java#36)

  test :
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.oneToTablePerClassOne_ownedByTarget()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#212)
    - single-table
        - [SingleTablePolymorphismReaderTest.oneToTablePerClassOne_ownedByTarget()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#212)
    - join-table
        - [JoinTablePolymorphismReaderTest.oneToTablePerClassOne_ownedByTarget()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#212)

- [x] with separate fetch :

  implementation :
    - [AggregateOneToManyWithMappedAssociationAppender.appendWithSeparateFetch(..)](AggregateOneToManyWithMappedAssociationAppender.java#128)
    - [AggregateOneToManyWithIndexedMappedAssociationAppender.appendWithSeparateFetch(..)](AggregateOneToManyWithIndexedMappedAssociationAppender.java#121)

  test:
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.oneToTablePerClassMany_ownedByTarget_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#629)
    - single-table
        - [SingleTablePolymorphismReaderTest.oneToTablePerClassMany_ownedByTarget_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#629)
    - join-table
        - [JoinTablePolymorphismReaderTest.oneToTablePerClassMany_ownedByTarget_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#629)
