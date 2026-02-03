package org.codefilarete.stalactite.dsl.relation;

import java.util.Collection;

public interface ManyToManyEntityOptions<C, O, S1 extends Collection<O>, S2 extends Collection<C>> extends ManyToManyOptions<C, O, S1, S2> {
	
	/**
	 * Defines the table name of the association table.
	 *
	 * Note that we don't define it for embeddable types (meaning putting this method in {@link ManyToManyOptions})
	 * because fixing the association table name for a reusable configuration means that all data will be stored in
	 * the same table which will cause name collision on foreign keys.
	 *
	 * @param tableName the table name of the association table
	 * @return the global mapping configurer
	 */
	ManyToManyJoinTableOptions<C, O, S1, S2> joinTable(String tableName);
}
