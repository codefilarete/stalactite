package org.gama.stalactite.persistence.engine.builder;

import java.util.List;

import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPointByMethodReference;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A {@link CascadeMany} dedicated to {@link List} to configure indexation.
 * 
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of O
 */
public class CascadeManyList<SRC, TRGT, TRGTID, C extends List<TRGT>> extends CascadeMany<SRC, TRGT, TRGTID, C> {
	
	private Column indexingColumn;
	
	public <T extends Table> CascadeManyList(IReversibleAccessor<SRC, C> collectionProvider, ValueAccessPointByMethodReference methodReference, EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration, T targetTable) {
		super(collectionProvider, methodReference, targetMappingConfiguration, targetTable);
	}
	
	public void setIndexingColumn(Column<? extends Table, Integer> indexingColumn) {
		this.indexingColumn = indexingColumn;
	}
	
	public Column getIndexingColumn() {
		return indexingColumn;
	}
}
