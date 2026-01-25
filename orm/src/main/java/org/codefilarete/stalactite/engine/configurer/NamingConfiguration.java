package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;

/**
 * Storage for all naming strategy.
 * Made to avoid having all those attributes in every class that requires them.
 * 
 * @author Guillaume Mary
 */
public class NamingConfiguration {
	
	private final TableNamingStrategy tableNamingStrategy;
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final UniqueConstraintNamingStrategy uniqueConstraintNamingStrategy;
	private final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private final MapEntryTableNamingStrategy mapEntryTableNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	
	public NamingConfiguration(TableNamingStrategy tableNamingStrategy,
							   ColumnNamingStrategy columnNamingStrategy,
							   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
							   UniqueConstraintNamingStrategy uniqueConstraintNamingStrategy,
							   ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
							   MapEntryTableNamingStrategy mapEntryTableNamingStrategy,
							   JoinColumnNamingStrategy joinColumnNamingStrategy,
							   ColumnNamingStrategy indexColumnNamingStrategy,
							   AssociationTableNamingStrategy associationTableNamingStrategy) {
		this.tableNamingStrategy = tableNamingStrategy;
		this.columnNamingStrategy = columnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.uniqueConstraintNamingStrategy = uniqueConstraintNamingStrategy;
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
		this.mapEntryTableNamingStrategy = mapEntryTableNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
	}
	
	public TableNamingStrategy getTableNamingStrategy() {
		return tableNamingStrategy;
	}
	
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return columnNamingStrategy;
	}
	
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return foreignKeyNamingStrategy;
	}
	
	public UniqueConstraintNamingStrategy getIndexNamingStrategy() {
		return uniqueConstraintNamingStrategy;
	}
	
	public ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy() {
		return elementCollectionTableNamingStrategy;
	}
	
	public MapEntryTableNamingStrategy getEntryMapTableNamingStrategy() {
		return mapEntryTableNamingStrategy;
	}
	
	public JoinColumnNamingStrategy getJoinColumnNamingStrategy() {
		return joinColumnNamingStrategy;
	}
	
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return indexColumnNamingStrategy;
	}
	
	public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
		return associationTableNamingStrategy;
	}
}
