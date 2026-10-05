Hereafter are the features that are supported by the resolver of many-to-one relations. This file also helps as a TODO list for current development.

| relation    | owner            | fetching  | polymorphic target | status |
|-------------|------------------|-----------|--------------------|--------|
| many-to-one | target           | join      | no                 | [x]    |
| many-to-one | target           | separate  | no                 | [x]    |
| ----------  | ---------------- | --------- | ------------------ | ------ |
| many-to-one | target           | join      | yes                | [x]    |
| many-to-one | target           | separate  | yes                | [x]    |



# many-to-one
## owned-by-source (default and only behavior)
The source entity is the owner of the relation (the referencing column is on the source table)
- [x] default use case :

  implementation :
    - write : [ManyToOneResolver.resolve(..)](ManyToOneResolver.java#43) → [ManyToOneEngine](ManyToOneEngine.java)
    - read : [AggregateManyToOneAppender.append(..)](AggregateManyToOneAppender.java#41)

  test : [ManyToOneResolverTest.multiple_manyToOne()](ManyToOneResolverTest.java#605)

- [x] with separate fetch (read) :

  implementation : [AggregateManyToOneAppender.append(..)](AggregateManyToOneAppender.java#69)
  test : [ManyToOneResolverTest.multiple_manyToOne_fetchSeparately()](ManyToOneResolverTest.java#688)

# one-to-polymorphic
Support for many-to-one relations with a polymorphic target (right side of the relation)

## owned-by-source (default and only behavior)
- [x] default use case :

  implementation : [AggregateManyToOneAppender.append(..)](AggregateManyToOneAppender.java#121)

  test :
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.ManyToOne.toTablePerClassOne()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#905)
    - single-table
        - [SingleTablePolymorphismReaderTest.ManyToOne.toTablePerClassOne()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#905)
    - join-table
        - [JoinTablePolymorphismReaderTest.ManyToOne.toTablePerClassOne()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#905)

- [x] with separate fetch :

  implementation : [AggregateManyToOneAppender.append(..)](AggregateManyToOneAppender.java#84)

  test:
    - table-per-class
        - [TablePerClassPolymorphismReaderTest.ManyToOne.toTablePerClassOne_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/tableperclass/TablePerClassPolymorphismReaderTest.java#990)
  - single-table
      - [SingleTablePolymorphismReaderTest.ManyToOne.toTablePerClassOne_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/singletable/SingleTablePolymorphismReaderTest.java#990)
  - join-table
      - [JoinTablePolymorphismReaderTest.ManyToOne.toTablePerClassOne_fetchSeparately()](../../../../../../../../../test/java/org/codefilarete/stalactite/engine/runtime/jointable/JoinTablePolymorphismReaderTest.java#990)
