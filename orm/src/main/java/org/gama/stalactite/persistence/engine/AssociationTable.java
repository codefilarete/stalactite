package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Database.Schema;
import org.gama.stalactite.persistence.structure.PrimaryKey;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class AssociationTable<SELF extends AssociationTable<SELF>> extends Table<SELF> {
	
	/**
	 * Column pointing to left table primary key.
	 * Expected to be joined with {@link #oneSidePrimaryKey}
	 */
	private final Column<SELF, Object> oneSideKeyColumn;
	
	/**
	 * Primary key (one column for now) of the source entities, on the (undefined) target table.
	 * Expected to be joined with {@link #oneSideKeyColumn}
	 */
	private final Column oneSidePrimaryKey;
	
	/**
	 * Column pointing to right table primary key.
	 * Expected to be joined with {@link #manySidePrimaryKey}
	 */
	private final Column<SELF, Object> manySideKeyColumn;
	
	/**
	 * Primary key (one column for now) of the collection entities, on the (undefined) target table
	 * Expected to be joined with {@link #manySideKeyColumn}
	 */
	private final Column manySidePrimaryKey;
	
	/**
	 * Primary key of this table, contains {@link #oneSideKeyColumn} and {@link #manySideKeyColumn}
	 */
	private final PrimaryKey<SELF> primaryKey;
	
	public AssociationTable(Schema schema,
							String name,
							Column oneSidePrimaryKey,
							Column manySidePrimaryKey,
							AssociationTableNamingStrategy namingStrategy,
							ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		super(schema, name);
		this.oneSidePrimaryKey = oneSidePrimaryKey;
		this.manySidePrimaryKey = manySidePrimaryKey;
		this.oneSideKeyColumn = addColumn(namingStrategy.giveOneSideColumnName(oneSidePrimaryKey), oneSidePrimaryKey.getJavaType()).primaryKey();
		this.manySideKeyColumn = addColumn(namingStrategy.giveManySideColumnName(manySidePrimaryKey), manySidePrimaryKey.getJavaType()).primaryKey();
		addForeignKey(foreignKeyNamingStrategy.giveName(oneSideKeyColumn, oneSidePrimaryKey), oneSideKeyColumn, oneSidePrimaryKey);
		addForeignKey(foreignKeyNamingStrategy.giveName(manySideKeyColumn, manySidePrimaryKey), manySideKeyColumn, manySidePrimaryKey);
		this.primaryKey = new PrimaryKey<>(oneSideKeyColumn, manySideKeyColumn);
	}
	
	public Column<SELF, Object> getOneSideKeyColumn() {
		return oneSideKeyColumn;
	}
	
	public Column getOneSidePrimaryKey() {
		return oneSidePrimaryKey;
	}
	
	public Column<SELF, Object> getManySideKeyColumn() {
		return manySideKeyColumn;
	}
	
	public Column getManySidePrimaryKey() {
		return manySidePrimaryKey;
	}
	
	@Nonnull
	@Override
	public PrimaryKey<SELF> getPrimaryKey() {
		return this.primaryKey;
	}
}
