## Iterating over them

You can iterate over a ResultSet with [ResultSetIterator](ResultSetIterator.java) or [RowIterator](RowIterator.java).
The former is the rawest way to convert a `ResultSet` because you'll be free to implement `convert(..)` : [ResultSetIterator](ResultSetIterator.java)
will "only" brings you the `Iterator` interface over a `ResultSet`.
So you may prefer to use [RowIterator](RowIterator.java) because it's still an `Iterator` but converts rows to [Row](Row.java)
 which is barely a `Map` of column names and their values, which is more "portable" and has no need of SQLException handling.
 The way columns are read from `ResultSet` comes from [ResultSetReader](../binder/binders.md#predefined-binders) given at construction time.
 
_Be aware that it is a memory overhead compared to [ResultSetIterator](ResultSetIterator.java)_


## Converting them to beans

The richest way to convert a `ResultSet` to beans is brought by [ResultSetConverter](ResultSetConverter.java) which will create a bean graph.
You may be interested in [ResultSetRowConverter](ResultSetRowConverter.java) for a simpler usage : convert each row of a `ResultSet` to a bean.