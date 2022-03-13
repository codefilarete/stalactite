# Core module of Stalactite

This module aims at providing a simple mapping between Beans and RDBMS tables.
**It doens't target relations management between them, except for querying**.

Main entry point of this module is [ClassMappingStrategy](Stalactite/stalactite/core/src/main/java/org/codefilarete/stalactite/mapping/ClassMappingStrategy.java) for persisting entities.
But you may also persist strange things like :
- [Collection of values](Stalactite/stalactite/core/src/main/java/org/codefilarete/stalactite/mapping/ColumnedCollectionMappingStrategy.java)
- [Map of values](Stalactite/stalactite/core/src/main/java/org/codefilarete/stalactite/mapping/ColumnedMapMappingStrategy.java)

You can query your database creating [Querys](src/main/java/org/codefilarete/stalactite/query/model/Query.java).
Combined with [QueryBuilder](src/main/java/org/codefilarete/stalactite/query/builder/SQLQueryBuilder.java) you can get executable SQL.
Here's a simple example with a [QueryEase](src/main/java/org/codefilarete/stalactite/query/model/QueryEase.java) static import:
<pre>
QueryProvider q = select(personTable.firstName, personTable.birthDate).from(personTable);
System.out.println(new QueryBuilder(q).toSQL())
// will print : select firstName, birthDate from Person; 
</pre>

A 2-tables-join one:
<pre>
Table personTable = new Table("Person");
Column<Long> personId = personTable.addColumn("id", Long.class);
Column<String> firstname = personTable.addColumn("firstname", String.class);
Column<String> lastname = personTable.addColumn("lastname", String.class);
Table carTable = new Table("Car");
Column<Long> ownerId = carTable.addColumn("owner", Long.class);
Column<String> carColor = carTable.addColumn("color", String.class);

QueryProvider q = select(firstname, lastname, carColor).from(personTable, "p").innerJoin(personId, ownerId).where(lastname, like("%jo%"));
System.out.println(new QueryBuilder(q).toSQL())
// will print : select p.firstname, p.lastname, c.color from Person as p inner join Car as c on p.id = c.owner where p.lastname like '%jo%'
</pre>

Please refer to [this test](src/test/java/org/codefilarete/stalactite/query/builder/SQLQueryBuilderTest.java) for examples.

# Caveat

Thus, by using this module you must have skills in schema design : you won't be helped in designing relations between tables to handle entity inheritance or entity relations : for all of this you should be interested in [ORM module](../orm/README.md).