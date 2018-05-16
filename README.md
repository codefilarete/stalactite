
Stalactite aims at providing both a SQL library and an ORM.

Its birth comes from a misconception on a huge domain-objects application (implying the creation of more than 200 database tables) and
exposing entities to the view. With a classical ORM this leads to a bothering fetch-all and cache-all solution.
Stalactite tries to ease loading of DTO or any adhoc-view-object, and insert/update/delete them into the database, without creating a huge
mapping configuration, trying to promote small object graphs in bounded context.

# Overview

The project is layered in 3 main modules to fullfill this goal:
- [sql](sql/README.md)
- [core](core/README.md)
- [orm](orm/README.md)

Here's an example of one can achieve with the ORM module, see [ORM module overview](orm/README.md) for detail:
<pre>
FluentMappingBuilder.from(MyEntity.class, StatefullIdentifier.class)
	.add(MyEntity::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
	.embed(MyEntity::getTimestamp)
		.overrideName(Timestamp::getCreationDate, "createdAt")
		.overrideName(Timestamp::getModificationDate, "modifiedAt")
	.build(persistenceContext);
</pre>

It's mainly based on a model of your RDBMS tables : the [core](core/README.md) module provides the [structure package](core/src/main/java/org/gama/stalactite/persistence/structure/README.md)
with the idea to create a meta-model of your database schema.

# Approach

Whereas some principles were kept from Hibernate such as [Dialects](core/src/main/java/org/gama/stalactite/persistence/sql/Dialects.md),
it doesn't have merge/attach notion, so it doesn't use any bytecode enhancement of your beans (for now and hope for a while).
The idea behind this is to let persist any bean that has no annotation, coming from "nowhere" such as a WebService DTO.

Allowing such a thing let you query your database and create DTOs for a particular view and then persist them without exposing your
rich-business-domain-model of your database (never knowing where your object graph reading have to stop), or copying it to DTOs with
boilerplate code (or dedicated framework).

# Features

## orm
- entities must implement [Identified](orm/src/main/java/org/gama/stalactite/persistence/id/Identified.java) which means that there id
must be a [Identifier](orm/src/main/java/org/gama/stalactite/persistence/id/Identifier.java), see foot note
- only supports single column primary key, see foot note
- only supports already-assigned identifier, see foot note
- one-to-one mapping
 	- property column owned by table owner or by reverse side
 	- eager : not lazy, select is done with join (no secondary select to load the linked entity)
- one-to-many mapping
	- property column owned by reverse side table
	- eager : not lazy, select is done with join (no secondary select to load the linked entities, prevents N+1 query)
- embedding of value class

###### Why already-assigned-identifier single column ?
- I prefer technical keys over domain ones because those may change over project life cycle. And overall, domain key values may change
(human error for instance) which might create nighmare when entities must deal with HashSet or HashMap.
- Already-assigned identifier allows to implement clean equals() & hashcode() based on it, here too it simpler when dealing with HashSet or HashMap
- Single column, because it was simpler at beginning ;)

## core
- [CRUD persistence](core/src/main/java/org/gama/stalactite/persistence/mapping/mapping.md)
- Listeners for persist actions on beans (before & after) : see [PersisterListener](core/src/main/java/org/gama/stalactite/persistence/engine/listening/PersisterListener.java),
accessible through `PersistenceContext.getPersisterListener()`
- SQL Query writing through [a fluent API](core/src/main/java/org/gama/stalactite/query/model/QueryEase.java)

## sql
- Transaction management thanks to [TransactionListener](sql/src/main/java/org/gama/sql/TransactionListener.java) which can be used
through a [TransactionAdapter](sql/src/main/java/org/gama/sql/TransactionAdapter.java)
- ResultSet iteration and transformation, see [ResultSet handling](sql/src/main/java/org/gama/sql/result/ResultSet%20handling.md)