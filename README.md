
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

It's mainly based on a model of your RDBMS tables : the [core](core/README.md) provides the [structure package](core/src/main/java/org/gama/stalactite/persistence/structure/README.md)
with the idea to create a meta-model of your database schema.

# Approach

Whereas some principles were kept from Hibernate such as [Dialects](core/src/main/java/org/gama/stalactite/persistence/sql/Dialects.md),
it doesn't have merge/attach notion, so it doesn't use any bytecode enhancement of your beans (for now and hope for a while).
The idea behind this is to let persist any bean that has no annotation, coming from "nowhere" such as a WebService DTO.

Allowing such a thing let you query your database and create DTOs for a particular view and then persist them without exposing your
rich-business-domain-model of your database (never knowing where your object graph reading have to stop), or copying it to DTOs with
boilerplate code (or dedicated framework).
