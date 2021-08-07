# Basics

* Only [Identified](../id/Identified.java) entities can be mapped
* Only entities with a [StatefullIdentifier](../../../../../../../../../core/src/main/java/org/gama/stalactite/persistence/id/manager/StatefullIdentifier.java) identifier can be mapped
* a [PersistenceContext](PersistenceContext.java) is necessary

## Mapping a POJO to a table

Given a `Country` POJO with an `id` property and a `name` property, both accessible throught a getter (and setter), one can write such a mapping:

<pre>
FluentMappingBuilder.from(MyEntity.class, StatefullIdentifier.class)
	.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
	.add(Country::getName)
	.addOneToOne(Country::getPresident, personPersister)
	.build(persistenceContext);
</pre>
which will map the `Country` class to a same-named table and columns.

With a more complex example that associates a `Country`, a `Person` as president, and some `City`s, you get :
<pre>
PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), new HSQLDBDialect());

Persister<Person, Identifier<Long>, ?> personMappingBuilder = FluentMappingBuilder.from(Person.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				.build(persistenceContext);
				
Persister<City, Identifier<Long>, ?> cityPersister = FluentMappingBuilder.from(City.class, Identifier.LONG_TYPE)
                                                     				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
                                                     				.add(City::getName)
                                                     				.build(persistenceContext);

Persister<City, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(MyEntity.class, StatefullIdentifier.class)
	.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
	.add(Country::getName)
	.addOneToOne(Country::getPresident, personPersister)
	.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry).cascading(ALL)
	.build(persistenceContext);
</pre>

With this configuration one shall give a [StatefullIdentifier](../../../../../../../../../core/src/main/java/org/gama/stalactite/persistence/id/manager/StatefullIdentifier.java) to its POJOs
before persisting them with the [PersistenceContext](PersistenceContext.java) due to the IdentifierPolicy.
Reader may have a look to [identifier generation policies](../../../../../../../../../core/src/main/java/org/gama/stalactite/persistence/id/manager/Identifier%20generation%20policies.md)

## Naming table, columns, foreign keys

Table names can be controlled by creating the right Table and passing it to the `from(..)` method of the [FluentMappingBuilder](FluentMappingBuilder.java).
As well as tables, colum names can be controlled by passing it to the `add` method of the [FluentMappingBuilder](FluentMappingBuilder.java).

Foreign keys are named according to a [ForeignKeyNamingStrategy](ForeignKeyNamingStrategy.java) that can be given to the [FluentMappingBuilder](FluentMappingBuilder.java)
through `foreignKeyNamingStrategy(..)` method.

Finally, in case of one-to-many with an association table (relation is not owned by target entities), association table name and its column names can
be specified by a [AssociationTableNamingStrategy](AssociationTableNamingStrategy.java).

## Mapping a composed bean

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

For some more examples you can have a look at those tests :
- [Cascading one to one](FluentMappingBuilderCascadeTest.java)
- [Cascading one to many](FluentMappingBuilderCollectionCascadeTest.java)
- [Versioning](FluentMappingBuilderVersioningTest.java)
- [Foreign key](FluentMappingBuilderForeignKeyTest.java)
