package org.gama.stalactite.persistence.engine.configurer;

import java.util.Set;

import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer.EntityInflater;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
class FirstPhaseOneToOneLoader<E, ID> implements EntityInflater<E, ID> {
	
	private final Column<Table, ID> primaryKey;
	private final IdMappingStrategy<E, ID> idMappingStrategy;
	private final ISelectExecutor<E, ID> selectExecutor;
	private final Class<E> mainType;
	private final ThreadLocal<Set<RelationIds<Object, Object, Object>>> relationIdsHolder;
	
	public FirstPhaseOneToOneLoader(IdMappingStrategy<E, ID> subEntityIdMappingStrategy,
									Column<Table, ID> primaryKey,
									ISelectExecutor<E, ID> selectExecutor,
									Class<E> mainType,
									ThreadLocal<Set<RelationIds<Object, Object, Object>>> relationIdsHolder) {
		this.primaryKey = primaryKey;
		this.idMappingStrategy = subEntityIdMappingStrategy;
		this.selectExecutor = selectExecutor;
		this.mainType = mainType;
		this.relationIdsHolder = relationIdsHolder;
	}
	
	@Override
	public Class<E> getEntityType() {
		return mainType;
	}
	
	@Override
	public ID giveIdentifier(Row row, ColumnedRow columnedRow) {
		return idMappingStrategy.getIdentifierAssembler().assemble(row, columnedRow);
	}
	
	@Override
	public AbstractTransformer<E> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return new AbstractTransformer<E>(null, columnedRow) {
			
			// this is not invoked
			@Override
			public AbstractTransformer<E> copyWithAliases(ColumnedRow columnedRow) {
				throw new UnsupportedOperationException("this is not expected to be copied, row transformation algorithm as changed,"
						+ " please fix it or fix this method");
			}
			
			@Override
			public void applyRowToBean(Row row, E bean) {
				fillCurrentRelationIds(row, bean, columnedRow);
			}
		};
	}
	
	protected void fillCurrentRelationIds(Row row, E bean, ColumnedRow columnedRow) {
		Set<RelationIds<Object, E, ID>> relationIds = (Set) relationIdsHolder.get();
		relationIds.add(new RelationIds<>(selectExecutor,
				idMappingStrategy.getIdAccessor()::getId, bean, (ID) columnedRow.getValue(primaryKey, row)));
	}
}
