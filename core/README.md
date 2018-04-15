# Core module of Stalactite

This module aims at providing a simple mapping between Beans and RDBMS tables.
**It doens't target relations management between them, except for querying**.

Main entry point of this module is [ClassMappingStrategy](src/main/java/org/gama/stalactite/persistence/mapping/ClassMappingStrategy.java).
And you can persist strange things like :
- [Collection of values](src/main/java/org/gama/stalactite/persistence/mapping/ColumnedCollectionMappingStrategy.java)
- [Map of values](src/main/java/org/gama/stalactite/persistence/mapping/ColumnedMapMappingStrategy.java)

You can query your database creating [Querys](src/main/java/org/gama/stalactite/query/model/Query.java).
Combined with [QueryBuilder](src/main/java/org/gama/stalactite/query/builder/QueryBuilder.java) you can get executable SQL.
Here's a simple example with a [QueryEase](src/main/java/org/gama/stalactite/query/model/QueryEase.java) static import:
<pre>
QueryProvider q = select(personTable.firstName, personTable.birthDate).from(personTable);
System.out.println(new QueryBuilder(q).toSQL())
// will print : select firstName, birthDate from Person; 
</pre>

A 2-table-join one:
<pre>
Table personTable = new Table("Person");
Column<Long> personId = personTable.addColumn("id", Long.class);
Column<String> firstname = personTable.addColumn("firstname", String.class);
Column<String> lastname = personTable.addColumn("lastname", String.class);
Table carTable = new Table("Car");
Column<Long> ownerId = carTable.addColumn("owner", Long.class);
Column<String> carColor = carTable.addColumn("color", String.class);

select(firstname, lastname, carColor).from(personTable, "p").innerJoin(personId, ownerId).where(lastname, like("%jo%"));
// select p.firstname, p.lastname, c.color from Person as p inner join Car as c on p.id = c.owner where p.lastname like '%jo%'
</pre>

Please refer to [this test](src/test/java/org/gama/stalactite/query/builder/QueryBuilderTest.java) for examples.

# 

Hence, using this module one must have skills in schema design, particularly for new table/bean : you won't be helped for designing
 relations between tables for one-to-many, many-to-one, etc. as would do JPA/Hibernate (it adds necessary columns, types, nullity, etc)

For relation management, one may be interested in having a look at the [ORM module](../orm/README.md)

# Further reading

Some more documentation can be found here
- [Dialects](src/main/java/org/gama/stalactite/persistence/sql/Dialects.md)
- [mapping](src/main/java/org/gama/stalactite/persistence/mapping/mapping.md)
- [about bean identifier](src/main/java/org/gama/stalactite/persistence/id/manager/Identifier generation policies.md)