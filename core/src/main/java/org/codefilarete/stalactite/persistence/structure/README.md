# Overview
A package for describing structure of a RDBMS schema.

One can create [Table](Table.java) and add [column](Column.java), then they can be used in the API for querying or persisting bean.
Schema can be enhanced by defining [ForeignKeys](ForeignKey.java) and [Indexes](Index.java) so they'll be added to DDL during
 [schema generation](../sql/ddl/DDLSchemaGenerator.java).

# Table (re)usage
It's encouraged to reuse table instances through treatments by declaring it once instead of duplicating declaration.
One can do such a thing:
<pre>
public class PersonTable extends Table {
	public final Column&lt;String&gt; firstName = addColumn(this, "firstName", String.class);
	public final Column&lt;Date&gt; birthDate = addColumn(this, "birthDate", Date.class);
	
	public PersonTable() {
		super("Person");
	}
}
</pre>

Finally you'll end up with a full meta-model of your database, which can be helpfull for refactoring phase.

**Be aware that no code generation is yet provided**