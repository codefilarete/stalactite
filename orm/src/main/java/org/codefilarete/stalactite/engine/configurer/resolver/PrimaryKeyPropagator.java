package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Creates a primary key equivalent to another one and creates a foreign key between them.
 *
 * @param <SRCTABLE> the type of the source table
 * @param <TARGETTABLE> the type of the target table
 * @param <I> the type of the primary key identifier
 * @author Guillaume Mary
 * @see #propagate(PrimaryKey, Table, ForeignKeyNamingStrategy)
 */
public class PrimaryKeyPropagator<SRCTABLE extends Table<SRCTABLE>, TARGETTABLE extends Table<TARGETTABLE>, I> {
	
	/**
	 * Creates a primary key on the given table by mimicking the given primary key.
	 * Also creates a foreign key between both. 
	 * 
	 * @param primaryKey the template primary key that must be applied to the target table
	 * @param target target table on which primary key and foreign key must be added
	 * @param foreignKeyNamingStrategy the naming strategy to use for the foreign key to create
	 */
	public void propagate(PrimaryKey<SRCTABLE, I> primaryKey,
	                      TARGETTABLE target,
	                      ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		// add primary key and foreign key to all tables
		projectPrimaryKey(primaryKey, target);
		addForeignKey(target, primaryKey, foreignKeyNamingStrategy);
	}
	
	/**
	 * Creates foreign keys between given tables primary keys.
	 *
	 * @param from target tables on which foreign keys must be added, <strong>order matters</strong>
	 * @param to initial primary key on which the very first table primary key must point to
	 */
	private void addForeignKey(TARGETTABLE from, PrimaryKey<SRCTABLE, I> to, ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		addForeignKey(from.getPrimaryKey(), to, foreignKeyNamingStrategy);
	}
	
	/**
	 * Creates the primary key on the given table by creating the equivalent columns with the name and type of the given primary key.
	 *
	 * @param source the template primary key that must be applied to the target table
	 * @param table target table on which the primary key must be added
	 */
	private void projectPrimaryKey(PrimaryKey<SRCTABLE, I> source, TARGETTABLE table) {
		source.getColumns().forEach(pkColumn -> {
			// nullability = false may not be necessary because of primary key, let for principle
			Column<TARGETTABLE, ?> newColumn = table.addColumn(pkColumn.getName(), pkColumn.getJavaType(), pkColumn.getSize(), false);
			newColumn.primaryKey();
		});
	}
	
	private void addForeignKey(PrimaryKey<TARGETTABLE, I> from, PrimaryKey<SRCTABLE, I> to, ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		from.getTable().addForeignKey(foreignKeyNamingStrategy.giveName(from, to), from, to);
	}
}
