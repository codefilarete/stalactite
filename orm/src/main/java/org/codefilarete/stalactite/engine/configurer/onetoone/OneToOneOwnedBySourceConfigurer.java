package org.codefilarete.stalactite.engine.configurer.onetoone;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.PassiveJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.onetoone.OneToOneOwnedBySourceEngine;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID> identifier type of target entities
 * @param <LEFTTABLE> left (source entity) table type
 * @param <RIGHTTABLE> right (target entity) table type
 * @param <JOINID> joining columns type
 * @author Guillaume Mary
 */
public class OneToOneOwnedBySourceConfigurer<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
		extends OneToOneConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE, JOINID> {
	
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	
	private final Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping = new HashMap<>();
	
	private OneToOneOwnedBySourceEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> engine;
	
	public OneToOneOwnedBySourceConfigurer(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									OneToOneRelation<SRC, TRGT, TRGTID> oneToOneRelation,
									JoinColumnNamingStrategy joinColumnNamingStrategy,
									ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		super(sourcePersister, oneToOneRelation);
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
	}
	
	@Override
	protected BeanRelationFixer<SRC, TRGT> determineRelationFixer() {
		Mutator<SRC, TRGT> targetSetter = oneToOneRelation.getTargetProvider().toMutator();
		return BeanRelationFixer.of(targetSetter::set);
	}
	
	@Override
	protected Duo<Key<LEFTTABLE, JOINID>, Key<RIGHTTABLE, JOINID>> determineForeignKeyColumns(EntityMapping<SRC, SRCID, LEFTTABLE> mappingStrategy,
																							  EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy) {
		Key<RIGHTTABLE, JOINID> rightKey = targetMappingStrategy.getTargetTable().getPrimaryKey();
		// adding foreign key constraint
		KeyBuilder<LEFTTABLE, JOINID> leftKeyBuilder = Key.from(mappingStrategy.getTargetTable());
		AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider());
		targetMappingStrategy.getTargetTable().getPrimaryKey().getColumns().forEach(column -> {
			String leftColumnName = joinColumnNamingStrategy.giveName(accessorDefinition, column);
			Column<LEFTTABLE, ?> foreignKeyColumn = mappingStrategy.getTargetTable().addColumn(leftColumnName, column.getJavaType());
			leftKeyBuilder.addColumn(foreignKeyColumn);
			keyColumnsMapping.put(foreignKeyColumn, column);
		});
		Key<LEFTTABLE, JOINID> leftKey = leftKeyBuilder.build();
		
		// According to the nullable option, we specify the ddl schema option
		leftKey.getColumns().forEach(c -> ((Column) c).nullable(oneToOneRelation.isNullable()));
		
		// we don't create foreign key for table-per-class because source columns should reference different tables (the one
		// per entity) which databases do not allow
		boolean createForeignKey = !oneToOneRelation.isTargetTablePerClassPolymorphic();
		if (createForeignKey) {
			String foreignKeyName = foreignKeyNamingStrategy.giveName(leftKey, rightKey);
			sourcePersister.<LEFTTABLE>getMapping().getTargetTable().addForeignKey(foreignKeyName, leftKey, rightKey);
		}
		
		return new Duo<>(leftKey, rightKey);
	}
	
	@Override
	protected void addSelectIn2Phases(
			String tableAlias,
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
			Key<LEFTTABLE, JOINID> leftKey,
			Key<RIGHTTABLE, JOINID> rightKey,
			FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		
		Table targetTableClone = new Table(targetPersister.getMapping().getTargetTable().getName());
		KeepOrderSet<Column> columns = (KeepOrderSet<Column>) (KeepOrderSet) rightKey.getColumns();
		columns.forEach(column -> targetTableClone.addColumn(column.getExpression(), column.getJavaType()).primaryKey());
		// This can't be done directly on root persister (took via persisterRegistry and targetPersister.getClassToPersist()) because
		// TransformerListener would get root instance as source (aggregate root), not current source
		String joinName = sourcePersister.getEntityJoinTree().addPassiveJoin(
				EntityJoinTree.ROOT_STRATEGY_NAME,
				leftKey,
				targetTableClone.getPrimaryKey(),
				tableAlias,
				oneToOneRelation.isNullable() ? JoinType.OUTER : JoinType.INNER,
				targetTableClone.getPrimaryKey().getColumns(),
				(src, columnValueProvider) -> firstPhaseCycleLoadListener.onFirstPhaseRowRead(src, targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnValueProvider)),
				true);
		
		// Propagating 2-phases load to all nodes that use cycling type
		PassiveJoinNode passiveJoin = (PassiveJoinNode) sourcePersister.getEntityJoinTree().getJoin(joinName);
		targetPersister.getEntityJoinTree().foreachJoin(joinNode -> {
			if (joinNode instanceof RelationJoinNode
					&& ((RelationJoinNode<?, ?, ?, ?, ?>) joinNode).getEntityInflater().getEntityType() == sourcePersister.getClassToPersist()) {
				EntityJoinTree.copyNodeToParent(passiveJoin, joinNode, leftKey);
			}
		});
	}
	
	@Override
	protected void addWriteCascades(ConfiguredPersister<TRGT, TRGTID> targetPersister) {
		this.engine = new OneToOneOwnedBySourceEngine<>(sourcePersister, targetPersister, oneToOneRelation.getTargetProvider(), keyColumnsMapping);
		boolean writeAuthorized = oneToOneRelation.getRelationMode() != RelationMode.READ_ONLY;
		if (writeAuthorized) {
			super.addWriteCascades(targetPersister);
		} else {
			// even if write is not authorized, we still have to insert and update source-to-target link, because we are in relation-owned-by-source
			this.engine.addForeignKeyMaintainer();
		}
	}
	
	@Override
	protected void addInsertCascade(ConfiguredPersister<TRGT, TRGTID> targetPersister) {
		super.addInsertCascade(targetPersister);
		engine.addInsertCascade();
	}
	
	@Override
	protected void addUpdateCascade(ConfiguredPersister<TRGT, TRGTID> targetPersister, boolean orphanRemoval) {
		super.addUpdateCascade(targetPersister, orphanRemoval);
		engine.addUpdateCascade(orphanRemoval);
	}
	
	@Override
	protected void addDeleteCascade(ConfiguredPersister<TRGT, TRGTID> targetPersister, boolean orphanRemoval) {
		engine.addDeleteCascade(orphanRemoval);
	}
}
