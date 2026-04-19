# Inheritance solving

This document describes the behavior of some features when they are combined with the `mapSuperClass(..)` method, what is called "inheritance" in this document.
The motivation for this clarification is that the first intention of `mapSuperClass(..)` is to apply the behavior of the given configuration to the current one. Therefore, the given configuration is considered shared between several mappings. However, the DSL also introduced secondary intentions, which can create conflicts or require clarification.
Here are the features affected by inheritance:
- naming conventions
- identifier mapping
- table resolution

This document doesn't deal with polymorphism, which is configured with the `mapPolymorphism(..)` method.

## Problems combining several `mapSuperClass(..)`
As a reminder, `mapSuperClass(..)` has two variations:
- one for entity inheritance, taking an [`entity configuration`](../../../dsl/entity/EntityMappingConfiguration.java) as argument
- one for embeddable inheritance, taking an [`embeddable configuration`](../../../dsl/embeddable/EmbeddableMappingConfiguration.java) as argument

In addition to sharing property mapping, which both do, the former is used for identifier mapping, table definition, and naming conventions, whereas the latter is used for naming conventions only.
Thus, because the DSL allows users to have multiple configurations calling `mapSuperClass(..)` (which is expected for domain models that heavily use inheritance), some explanations about potential conflict resolution are necessary. Some features can be overridden, some are forbidden, and some are ignored.

Note that an entity can inherit any number of times from another entity and/or an embeddable, but an embeddable can only inherit from another embeddable (because inheriting from an entity would make it an entity itself). A possible inheritance chain can be represented as follows:
```
// with "->" meaning "inherits from"
entity E -> entity D -> entity C -> embeddable B -> embeddable A
```
Because `embeddable B -> entity A` is not valid (and prevented by the DSL).

In short:
1. arbitrary inheritance mixing is not allowed
2. once the chain switches to embeddables, the rest of the chain must be embeddables

## Naming conventions
This section refers to the DSL `with*Naming(..)` methods. This is a simple case: the conventional inheritance rule applies. A configuration `X` using `withTableNaming(..)` passes its naming strategy to subclasses that call `mapSuperClass(X)`. If a subclass configuration redefines it, then it overwrites it for it and its descendant.

## Identifier mapping
This section refers to the DSL `mapKey(..)` methods (available only on entity configurations). This is also a simple case: the system is close to conventional inheritance rules. However, it differs on 2 points:
1. strictly, we don't apply the same strategy to subclasses (because, for example, having several sequences would make no sense), but we consider it defined for them: in particular they will have to refer to it to create the identifier.
2. in a `mapSuperClass(..)` hierarchy, it is forbidden to have several configurations using it (a configuration error is raised), hence a subclass configuration cannot redefine it.

For example, in
```
// with "->" meaning "inherits from"
entity E -> entity D -> entity C -> embeddable B -> embeddable A
```
If D defines the identifier, then:
- E benefits from it (because E applies `mapSuperClass(D)`)
- C does not see it: C is an independent configuration and does not know what D defines after it. If C is reused as the root of another hierarchy, it must define its own identifier.
- B and A are not concerned: as embeddables, they cannot structurally carry an identifier.

## Table resolution
This section refers to the DSL `onTable(..)` methods combined with `joiningTables()` (callable only after `mapSuperClass(..)`).

1. without any `joiningTables()`, if the entity that starts the mapping uses `onTable(..)`, all properties are stored in this table, and any `onTable(..)` on upper classes is ignored. This is JPA behavior.
2. if `joiningTables()` is called between X and Y, then 2 tables are created for X and Y respectively, honoring `onTable(..)` on X and Y or the applicable naming convention. The join is done on the primary key of both tables, which means the identifier policy must be declared upstream (i.e. in upper classes). If no policy is deduced for Y then an error will be thrown. For example:
```
// with "->" meaning "inherits from"
// joiningTables() is called only between X and Y
entity X -> entity Y -> entity Z
```
- Y or exclusively Z must define the identifier policy, if none is found then an error will be thrown
- one table is created for X, one table is created for Y
- no table is created for Z, even if `onTable(..)` is used on it (the call is silently ignored)
- Z properties go into Y table

If, on the contrary, `joiningTables()` is also declared between Y and Z, then a third table is created for Z and its properties are stored there:
```
// with "->" meaning "inherits from with joiningTables()"
entity X -> entity Y -> entity Z
```
- one table for X (X properties), one table for Y (Y properties), one table for Z (Z properties)
- the identifier policy must be defined on Z (the root of the hierarchy)
