package org.codefilarete.stalactite.engine.runtime;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy.ReferencedColumnNames;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public class AssociationTable<
		SELF extends AssociationTable<SELF, LEFTTABLE, RIGHTTABLE, LEFTID, RIGHTID>,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		LEFTID,
		RIGHTID>
		extends Table<SELF> {
	
	/**
	 * Foreign key pointing to left table primary key
	 * Expected to be joined with {@link #oneSideKey}
	 */
	private final Key<SELF, LEFTID> oneSideForeignKey;
	
	/**
	 * Primary key of source entities table
	 * Expected to be joined with {@link #oneSideForeignKey}
	 */
	private final PrimaryKey<LEFTTABLE, LEFTID> oneSideKey;
	
	/**
	 * Foreign key pointing to right table primary key
	 * Expected to be joined with {@link #manySideKey}
	 */
	private final Key<SELF, RIGHTID> manySideForeignKey;
	
	/**
	 * Primary key of collection entities table
	 * Expected to be joined with {@link #manySideForeignKey}
	 */
	private final PrimaryKey<RIGHTTABLE, RIGHTID> manySideKey;
	
	private final Map<Column<LEFTTABLE, ?>, Column<SELF, ?>> leftIdentifierColumnMapping = new HashMap<>();
	
	private final Map<Column<RIGHTTABLE, ?>, Column<SELF, ?>> rightIdentifierColumnMapping = new HashMap<>();
	
	public AssociationTable(Schema schema,
							String name,
							PrimaryKey<LEFTTABLE, LEFTID> oneSideKey,
							PrimaryKey<RIGHTTABLE, RIGHTID> manySideKey,
							AccessorDefinition accessorDefinition,
							AssociationTableNamingStrategy namingStrategy,
							ForeignKeyNamingStrategy foreignKeyNamingStrategy,
							boolean createOneSideForeignKey,
							boolean createManySideForeignKey) {
		super(schema, name);
		this.oneSideKey = oneSideKey;
		this.manySideKey = manySideKey;
		ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> columnNames = namingStrategy.giveColumnNames(accessorDefinition, oneSideKey, manySideKey);
		KeyBuilder<SELF, LEFTID> leftForeignKeyBuilder = Key.from((SELF) this);
		oneSideKey.getColumns().forEach(oneSideKeyColumn -> {
			Column<SELF, ?> column = addColumn(columnNames.getLeftColumnName(oneSideKeyColumn), oneSideKeyColumn.getJavaType());
			column.primaryKey();
			leftForeignKeyBuilder.addColumn(column);
			leftIdentifierColumnMapping.put(oneSideKeyColumn, column);
		});
		Key<SELF, LEFTID> leftForeignKey = leftForeignKeyBuilder.build();
		if (createOneSideForeignKey) {
			this.oneSideForeignKey = addForeignKey(foreignKeyNamingStrategy::giveName, leftForeignKey, oneSideKey);
		} else {
			this.oneSideForeignKey = leftForeignKey;
		}
		
		// building many side key (eventually foreign key) 
		KeyBuilder<SELF, RIGHTID> rightForeignKeyBuilder = Key.from((SELF) this);
		manySideKey.getColumns().forEach(manySideKeyColumn -> {
			Column<SELF, ?> column = addColumn(columnNames.getRightColumnName(manySideKeyColumn), manySideKeyColumn.getJavaType());
			column.primaryKey();
			rightForeignKeyBuilder.addColumn(column);
			rightIdentifierColumnMapping.put(manySideKeyColumn, column);
		});
		Key<SELF, RIGHTID> rightForeignKey = rightForeignKeyBuilder.build();
		if (createManySideForeignKey) {
			this.manySideForeignKey = addForeignKey(foreignKeyNamingStrategy::giveName, rightForeignKey, manySideKey);
		} else {
			this.manySideForeignKey = rightForeignKey;
		}
	}
	
	public AssociationTable(Schema schema,
							String name,
							PrimaryKey<LEFTTABLE, LEFTID> oneSideKey,
							PrimaryKey<RIGHTTABLE, RIGHTID> manySideKey,
							AccessorDefinition accessorDefinition,
							AssociationTableNamingStrategy namingStrategy,
							ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this(schema, name, oneSideKey, manySideKey, accessorDefinition, namingStrategy, foreignKeyNamingStrategy, true, true);
	}
	
	/**
	 * Gives the foreign key pointing to left table primary key. Expected to be joined with {@link #getOneSideKey()} 
	 * @return the foreign key pointing to left table primary key
	 */
	public Key<SELF, LEFTID> getOneSideForeignKey() {
		return oneSideForeignKey;
	}
	
	/**
	 * Gives the primary key of source entities table. The one expected to be joined with {@link #getOneSideForeignKey()}
	 * @return the primary key of source entities table
	 */
	public Key<LEFTTABLE, LEFTID> getOneSideKey() {
		return oneSideKey;
	}
	
	/**
	 * Gives the foreign key pointing to right table primary key. Expected to be joined with {@link #getManySideKey()}
	 * @return the foreign key pointing to right table primary key
	 */
	public Key<SELF, RIGHTID> getManySideForeignKey() {
		return manySideForeignKey;
	}
	
	/**
	 * Gives the primary key of collection entities table. The one expected to be joined with {@link #getManySideForeignKey()}
	 * @return the primary key of collection entities table
	 */
	public Key<RIGHTTABLE, RIGHTID> getManySideKey() {
		return manySideKey;
	}
	
	public Map<Column<LEFTTABLE, ?>, Column<SELF, ?>> getLeftIdentifierColumnMapping() {
		return leftIdentifierColumnMapping;
	}
	
	public Map<Column<RIGHTTABLE, ?>, Column<SELF, ?>> getRightIdentifierColumnMapping() {
		return rightIdentifierColumnMapping;
	}
}
