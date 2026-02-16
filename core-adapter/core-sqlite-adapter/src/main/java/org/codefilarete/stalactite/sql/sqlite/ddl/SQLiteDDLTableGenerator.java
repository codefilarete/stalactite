package org.codefilarete.stalactite.sql.sqlite.ddl;


import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ddl.structure.UniqueConstraint;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class SQLiteDDLTableGenerator extends DDLTableGenerator {
	
	public SQLiteDDLTableGenerator(SqlTypeRegistry typeMapping, DMLNameProviderFactory dmlNameProviderFactory) {
		super(typeMapping, dmlNameProviderFactory);
	}
	
	/**
	 * Overridden to take the way SQLite adds a unique constraint : through an index
	 * @param uniqueConstraint the unique constraint to add
	 * @return the generated DDL order to add the unique constraint
	 */
	@Override
	public String generateCreateUniqueConstraint(UniqueConstraint<?> uniqueConstraint) {
		Table table = uniqueConstraint.getTable();
		StringAppender sqlCreateIndex = new DDLAppender(dmlNameProvider, "create unique index ")
				.cat(uniqueConstraint.getName(), " on ", table, "(")
				.ccat(uniqueConstraint.getColumns(), ", ");
		return sqlCreateIndex.cat(")").toString();
	}
	
	@Override
	public String generateCreateForeignKey(ForeignKey<?, ?, ?> foreignKey) {
		// SQLite doesn't support any kind of foreign key creation out of the table creation SQL statement
		// The only way to add the foreign key is to recreate the table with it : hence the process is a bit complex
		// since you have to create a duplicate of your table, with the foreign key, copy the data to it, remove the
		// old table. Obviously this can't be done here and if left to the user-dev.
		throw new UnsupportedOperationException("SQLite doesn't support foreign key creation out of create table");
	}
}
