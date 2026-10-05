Hereafter are the features that are supported by the resolver of one-to-one relations. This file also helps as a TODO list for current development.

| relation   | owner            | fetching  | polymorphic target | status |
|------------|------------------|-----------|--------------------|--------|
| one-to-one | source (default) | join      | no                 | [x]    |
| one-to-one | source (default) | separate  | no                 | [x]    |
| one-to-one | target           | join      | no                 | [x]    |
| one-to-one | target           | separate  | no                 | [x]    |
| ---------- | ---------------- | --------- | ------------------ | ------ |
| one-to-one | source (default) | join      | yes                | [x]    |
| one-to-one | source (default) | separate  | yes                | [x]    |
| one-to-one | target           | join      | yes                | [x]    |
| one-to-one | target           | separate  | yes                | [x]    |



# one-to-one
## owned-by-source (default behavior)
The source entity is the owner of the relation (the referencing column is on the source table)
- [x] default use case :

    implementation :
    - write : [OneToOneResolver.resolve(..)](OneToOneResolver.java#45) → [OneToOneOwnedBySourceEngine](OneToOneOwnedBySourceEngine.java)
    - read : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#132)
    
    test : [OneToOneResolverTest](OneToOneResolverTest.java)

- [x] with separate fetch (read) :

    implementation : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#77), [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#101)

    test : [OneToOneResolverTest.Multiple_OneToOne](OneToOneResolverTest.java#725)

## owned-by-target
The target entity is the owner of the relation (the referencing column is on the target table)
- [x] default use case :

  implementation :
    - write : [OneToOneResolver.resolve(..)](OneToOneResolver.java#45) → [OneToOneOwnedByTargetEngine](OneToOneOwnedByTargetEngine.java)
    - read : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#136)

  test : [OneToOneResolverTest](OneToOneResolverTest.java)

- [x] with separate fetch (read) :

  implementation : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#63)

# one-to-polymorphic
Support for one-to-one relations with a polymorphic target (right side of the relation)
## owned-by-source (default behavior)
- [x] default use case :

  implementation : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#121)

  test :
  - table-per-class
    - [TablePerClassPolymorphismReaderTest.oneToTablePerClassOne()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#82)
  - single-table
    - [SingleTablePolymorphismReaderTest.oneToSingleTableOne()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#82)
  - join-table
    - [JoinTablePolymorphismReaderTest.oneToJoinTableOne()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#82)

- [x] with separate fetch :

  implementation : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#84)

  test:
  - table-per-class
    - [TablePerClassPolymorphismReaderTest.oneToTablePerClassOne_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#146)
  - single-table
      - [SingleTablePolymorphismReaderTest.oneToSingleTableOne_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#146)
  - join-table
      - [JoinTablePolymorphismReaderTest.oneToJoinTableOne_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#146)

## owned-by-target
- [x] default use case :

  implementation : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#121)
  (same as owned-by-source)

  test :
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.oneToTablePerClassOne_ownedByTarget()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#211)
    - single-table
        - [SingleTablePolymorphismReaderTest.oneToSingleTableOne_ownedByTarget()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#211)
    - join-table
        - [JoinTablePolymorphismReaderTest.oneToJoinTableOne_ownedByTarget()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#211)

- [x] with separate fetch :

  implementation : [AggregateOneToOneAppender.append(..)](AggregateOneToOneAppender.java#84)

  test:
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.oneToTablePerClassOne_ownedByTarget_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#283)
    - single-table
        - [SingleTablePolymorphismReaderTest.oneToSingleTableOne_ownedByTarget_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#283)
    - join-table
        - [JoinTablePolymorphismReaderTest.oneToJoinTableOne_ownedByTarget_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#283)