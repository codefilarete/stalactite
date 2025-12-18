package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;

public class PrimaryKeyPropagationStep<C, I> {
	
	
	<T extends Table<T>> void propagate(PrimaryKey<T, I> primaryKey,
										MappingPerTable<C> inheritanceMappingPerTable,
										ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Set<Table> inheritanceTables = inheritanceMappingPerTable.giveTables();
		inheritanceTables.remove(primaryKey.getTable());
		// add primary key and foreign key to all tables
		propagatePrimaryKey(primaryKey, inheritanceTables);
		List<Table> tables = new ArrayList<>(inheritanceTables);
		Collections.reverse(tables);
		applyForeignKeys(primaryKey, new KeepOrderSet<>(tables), foreignKeyNamingStrategy);
	}
	
	/**
	 * Creates foreign keys between given tables primary keys.
	 *
	 * @param primaryKey initial primary key on which the very first table primary key must point to
	 * @param tables target tables on which foreign keys must be added, <strong>order matters</strong>
	 */
	@VisibleForTesting
	void applyForeignKeys(PrimaryKey primaryKey, Set<Table> tables, ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		applyForeignKeys(primaryKey, foreignKeyNamingStrategy, tables);
	}
	
	/**
	 * Creates primary keys on given tables with name and type of given primary key.
	 *
	 * @param tables target tables on which primary keys must be added
	 * @param primaryKey
	 */
	public static void propagatePrimaryKey(PrimaryKey<?, ?> primaryKey, Set<Table> tables) {
		Holder<PrimaryKey<?, ?>> previousPk = new Holder<>(primaryKey);
		tables.forEach(t -> {
			previousPk.get().getColumns().forEach(pkColumn -> {
				// nullability = false may not be necessary because of primary key, let for principle
				Column newColumn = t.addColumn(pkColumn.getName(), pkColumn.getJavaType(), pkColumn.getSize(), false);
				newColumn.primaryKey();
			});
			previousPk.set(t.getPrimaryKey());
		});
	}
	
	public static void applyForeignKeys(PrimaryKey primaryKey, ForeignKeyNamingStrategy foreignKeyNamingStrategy, Set<Table> tables) {
		Holder<PrimaryKey> previousPk = new Holder<>(primaryKey);
		tables.forEach(t -> {
			PrimaryKey currentPrimaryKey = t.getPrimaryKey();
			t.addForeignKey(foreignKeyNamingStrategy.giveName(currentPrimaryKey, previousPk.get()), currentPrimaryKey, previousPk.get());
			previousPk.set(currentPrimaryKey);
		});
	}
}
