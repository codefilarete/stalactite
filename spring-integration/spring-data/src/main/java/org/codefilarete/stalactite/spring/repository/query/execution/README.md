# About derived query execution
Hereafter is explained why there are so many classes in this package and what are their purpose.

# TL;DR
The first naïve global pattern of a derived query execution is :
1. create the query executor (based on Stalactite EntityFinder) with the criteria of the method, with eventual sorting
2. apply some windowing (slice, page, etc)
3. eventually convert its result to handle either domain entity or projection

You will find this pattern in the [AbstractRepositoryQuery](AbstractRepositoryQuery.java) class, in the *execute(Object[])* method:
1. buildQueryExecutor
2. buildResultReducer
3. buildResultProcessor

However, the fine approach is much more complex and is detailed here below.

# With some details
Spring Data has a lot of features, overall if we combine them. For our purpose, we will focus on the selection case, not the deletion.
Let's start we the obvious ones :
- from the method name, the user may define the criteria and the sorting, as in findByNameAndAgeSortingByAgeDesc()
- the windowing (limit and offset) is *mainly* done by the return type: if your method returns a Slice, then some reduction must be performed. However, the method name may directly set some constraint, as in findFirstByAge(): the "window" is the only first element of the result.

Let's add the possibility to define a projection: now, out of a default and basic behavior that selects the domain entities, we have to customize the query generator with the columns to select. Hence, the window clauses must also be applied to it.

With [@NativeQuery](../NativeQuery.java) comes the first problem because we have to adjust the limit and offset clauses of the provided SQL, either for domain entities as well as for projections. Thus, the query provider must give a "hook" to let us adapt the SQL.

To add a bit of complexity, reader must know that Page is a particular case of Spring Slice that required to know the total number of elements retrieved by the criteria. Hence, in some circumstances Stalactite will ask a count query to make it faisable.

But the real nightmare comes with the possibility to make dynamic projections: an underestimated Spring feature is its ability to define the type of the result dynamically, through a class parameter of the method signature. And the domain entity is allowed, which is no more a projection ! and make it fall back to a default behavior, at the very last instant of execution.

Though, trying to summarize, we have several query providers:
- default case based on Stalactite EntityFinder
- projection case, still base on Stalactite EntityFinder
- native query case
- Spring Bean query case

They must be combined to windowing, either "hardcoded" through method name or "dynamically" through the return type: slice, page, stream, etc (don't forget the Page particularity that requires count).

Finally, this is consumed by the builder which handle projection into account if necessary.

All this wasn't easy to design, and I leveraged Object Oriented programming to make this happen, with sometimes a lot of abstraction layers. The global design is also the result of an unplanned process with the discovery of Spring's features, hence it may appear a bit chaotic.

Hoping this small explanation helps anyone who will try to understand the Stalactite Spring Data integration.
