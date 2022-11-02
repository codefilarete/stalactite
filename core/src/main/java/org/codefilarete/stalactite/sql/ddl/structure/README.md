# Overview
A package aimed at describing structure of a RDBMS schema.

[Tables](Table.java) and their [Columns](Column.java) are principal elements. They can be used in the API for querying or persisting bean.
Schema can be enhanced by defining [ForeignKeys](ForeignKey.java), [Indexes](Index.java) and [UniqueConstraints](UniqueConstraint.java) so they'll be added to DDL during
 [schema generation](../sql/ddl/DDLSchemaGenerator.java).

# Table (re)usage
It's encouraged to reuse table instances through treatments by declaring it once instead of duplicating declaration.
One can do such a thing:
<pre>
public class PersonTable extends Table&lt;PersonTable&gt; {
	public final Column&lt;String&gt; firstName = addColumn(this, "firstName", String.class);
	public final Column&lt;Date&gt; birthDate = addColumn(this, "birthDate", Date.class);
	
	public PersonTable() {
		super("Person");
	}
}
</pre>

By doing this for all of your tables, you'll finally end up with a full meta-model of your database, then those tables and columns can be referenced in queries and persistence configuration to enforce type matching, avoid naming errors, as well as enhance your code robustness to refactoring.


**Be aware that no code generation is yet provided**