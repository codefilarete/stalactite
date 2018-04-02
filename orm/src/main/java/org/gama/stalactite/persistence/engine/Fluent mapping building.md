# Basics

* Only [Identified](../id/Identified.java) entities can be mapped
* Only entities with a [StatefullIdentifier](../../../../../../../../../core/src/main/java/org/gama/stalactite/persistence/id/manager/StatefullIdentifier.java) identifier can be mapped
* a [PersistenceContext](PersistenceContext.java) is necessary

## Mapping a POJO to a table

Given a `MyEntity` POJO with an `id` property and a `name` property, both accessible throught a getter (and setter), one can write such a mapping:

<pre>
FluentMappingBuilder.from(MyEntity.class, StatefullIdentifier.class)
	.add(MyEntity::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
	.add(MyEntity::getName)
	.build(persistenceContext);
</pre>

which will map the `MyEntity` class to a same-named table and columns.
With this configuration one shall give a [StatefullIdentifier](../../../../../../../../../core/src/main/java/org/gama/stalactite/persistence/id/manager/StatefullIdentifier.java) to its POJOs
before persisting them with the [PersistenceContext](PersistenceContext.java) due to the IdentifierPolicy.
Reader may have a look to [identifier generation policies](../../../../../../../../../core/src/main/java/org/gama/stalactite/persistence/id/manager/Identifier%20generation%20policies.md)

## Applying a naming strategy

To be implemented

## Mapping an composed bean

Given a `MyEntity` POJO with an `id` property and a `Timestamp` property composed of two other properties `creationDate` and `modificationDate`,
 accessible throught a getter (and setter), one can write such a mapping:

<pre>
FluentMappingBuilder.from(MyEntity.class, StatefullIdentifier.class)
	.add(MyEntity::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
	.embed(MyEntity::getTimestamp)
		.overrideName(Timestamp::getCreationDate, "createdAt")
		.overrideName(Timestamp::getModificationDate, "modifiedAt")
	.build(persistenceContext);
</pre>

For some more examples you can have a look at tests.