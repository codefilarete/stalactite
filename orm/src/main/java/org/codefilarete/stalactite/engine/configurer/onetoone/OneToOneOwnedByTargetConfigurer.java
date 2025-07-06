package org.codefilarete.stalactite.engine.configurer.onetoone;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.PassiveJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.RelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.onetoone.OneToOneOwnedByTargetEngine;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID> identifier type of target entities
 * @param <LEFTTABLE> left (source entity) table type
 * @param <RIGHTTABLE> right (target entity) table type
 * @author Guillaume Mary
 */
public class OneToOneOwnedByTargetConfigurer<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends OneToOneConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE, SRCID> {
	
	private final Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping = new HashMap<>();
	
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private OneToOneOwnedByTargetEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> engine;
	
	/**
	 * Right table column for join, may be right table primary key or column pointing to left table primary key.
	 * Stored as attribute because it is used out of case designed by parent class : for foreign key maintenance in
	 * read-only case.
	 */
	private Key<RIGHTTABLE, SRCID> rightKey;
	
	public OneToOneOwnedByTargetConfigurer(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
										   OneToOneRelation<SRC, TRGT, TRGTID> oneToOneRelation,
										   JoinColumnNamingStrategy joinColumnNamingStrategy,
										   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
										   Dialect dialect,
										   ConnectionConfiguration connectionConfiguration) {
		super(sourcePersister, oneToOneRelation);
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	@Override
	protected Duo<Key<LEFTTABLE, SRCID>, Key<RIGHTTABLE, SRCID>> determineForeignKeyColumns(EntityMapping<SRC, SRCID, LEFTTABLE> mappingStrategy,
																							EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy) {
		// left column is always left table primary key
		Key<LEFTTABLE, SRCID> leftKey = mappingStrategy.getTargetTable().getPrimaryKey();
		// right column depends on relation owner
		if (oneToOneRelation.getReverseColumn() != null) {
			rightKey = Key.ofSingleColumn(oneToOneRelation.getReverseColumn());
			PrimaryKey<LEFTTABLE, SRCID> sourcePrimaryKey = sourcePersister.<LEFTTABLE>getMainTable().getPrimaryKey();
			if (sourcePrimaryKey.isComposed()) {
				throw new UnsupportedOperationException("Can't map composite primary key " + sourcePrimaryKey + " on single reverse foreign key : " + oneToOneRelation.getReverseColumn());
			} else {
				keyColumnsMapping.put(sourcePrimaryKey.getColumns().getAt(0), oneToOneRelation.getReverseColumn());
			}
		}
		if (oneToOneRelation.getReverseGetter() != null) {
			AccessorByMethodReference<TRGT, SRC> localReverseGetter = Accessors.accessorByMethodReference(oneToOneRelation.getReverseGetter());
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(localReverseGetter);
			// we add a column for reverse mapping if one is not already declared
			rightKey = createOrUseReverseColumn(targetMappingStrategy, oneToOneRelation.getReverseColumn(), localReverseGetter, accessorDefinition);
		} else if (oneToOneRelation.getReverseSetter() != null) {
			ValueAccessPoint<TRGT> reverseSetter = Accessors.mutatorByMethodReference(oneToOneRelation.getReverseSetter());
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(reverseSetter);
			// we add a column for reverse mapping if one is not already declared
			rightKey = createOrUseReverseColumn(targetMappingStrategy, oneToOneRelation.getReverseColumn(), reverseSetter, accessorDefinition);
		}
		
		// adding foreign key constraint
		// we don't create foreign key for table-per-class because source columns should reference different tables (the one
		// per entity) which databases do not allow
		boolean createForeignKey = !oneToOneRelation.isTargetTablePerClassPolymorphic();
		if (createForeignKey) {
			String foreignKeyName = foreignKeyNamingStrategy.giveName(rightKey, leftKey);
			// Note that rightColumn can't be null because RelationOwnedByTargetConfigurer is used when one of cascadeOne.getReverseColumn(),
			// cascadeOne.getReverseGetter() and cascadeOne.getReverseSetter() is not null
			((Table) rightKey.getTable()).addForeignKey(foreignKeyName, rightKey, leftKey);
		}
		
		return new Duo<>(leftKey, rightKey);
	}
	
	private Key<RIGHTTABLE, SRCID> createOrUseReverseColumn(
			EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy,
			Column<RIGHTTABLE, SRCID> reverseColumn,
			ValueAccessPoint<TRGT> reverseGetter,
			AccessorDefinition accessorDefinition) {
		if (reverseColumn == null) {
			// no reverse column was given, so we look for the one mapped under the reverse getter
			reverseColumn = (Column<RIGHTTABLE, SRCID>) targetMappingStrategy.getPropertyToColumn().get(reverseGetter);
			if (reverseColumn == null) {
				// no column is defined under reverse getter, then we have to create one
				PrimaryKey<LEFTTABLE, SRCID> sourcePrimaryKey = sourcePersister.<LEFTTABLE>getMainTable().getPrimaryKey();
				KeyBuilder<RIGHTTABLE, SRCID> result = Key.from(targetMappingStrategy.getTargetTable());
				sourcePrimaryKey.getColumns().forEach(pkColumn -> {
					Column<RIGHTTABLE, ?> column = targetMappingStrategy.getTargetTable().addColumn(
							joinColumnNamingStrategy.giveName(accessorDefinition, pkColumn),
							pkColumn.getJavaType());
					keyColumnsMapping.put(pkColumn, column);
					result.addColumn(column);
				});
				return result.build();
			}
		}
		return Key.ofSingleColumn(reverseColumn);
	}
	
	@Override
	protected void addWriteCascades(ConfiguredPersister<TRGT, TRGTID> targetPersister) {
		this.engine = new OneToOneOwnedByTargetEngine<>(sourcePersister, targetPersister, oneToOneRelation.getTargetProvider(), keyColumnsMapping);
		boolean writeAuthorized = oneToOneRelation.getRelationMode() != RelationMode.READ_ONLY;
		if (writeAuthorized) {
			super.addWriteCascades(targetPersister);
		} else {
			this.engine.addForeignKeyMaintainer(dialect, connectionConfiguration, rightKey);
		}
	}
	
	@Override
	protected BeanRelationFixer<SRC, TRGT> determineRelationFixer() {
		Mutator<SRC, TRGT> sourceIntoTargetFixer = oneToOneRelation.getTargetProvider().toMutator();
		BeanRelationFixer<SRC, TRGT> result;
		
		if (oneToOneRelation.getReverseGetter() != null) {
			AccessorByMethodReference<TRGT, SRC> localReverseGetter = Accessors.accessorByMethodReference(oneToOneRelation.getReverseGetter());
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(localReverseGetter);
			
			// we take advantage of foreign key computing and presence of AccessorDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
			Mutator<TRGT, SRC> targetIntoSourceFixer = Accessors.mutatorByMethod(accessorDefinition.getDeclaringClass(), accessorDefinition.getName());
			result = (src, target) -> {
				// fixing source on target
				if (target != null) {    // prevent NullPointerException, actually means no linked entity (null relation), so nothing to do
					targetIntoSourceFixer.set(target, src);
				}
				// fixing target on source
				sourceIntoTargetFixer.set(src, target);
			};
		} else if (oneToOneRelation.getReverseSetter() != null) {
			// we take advantage of foreign key computing and presence of AccessorDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
			result = (target, input) -> {
				// fixing target on source side
				oneToOneRelation.getReverseSetter().accept(input, target);
				// fixing source on target side
				sourceIntoTargetFixer.set(target, input);
			};
		} else {
			// non bidirectional relation : relation is owned by target without defining any way to fix it in memory
			// we can only fix target on source side
			result = sourceIntoTargetFixer::set;
		}
		
		return result;
	}
	
	@Override
	protected void addSelectIn2Phases(
			String tableAlias,
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
			Key<LEFTTABLE, SRCID> leftKey,
			Key<RIGHTTABLE, SRCID> rightKey,
			FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		
		RIGHTTABLE targetTable = targetPersister.<RIGHTTABLE>getMapping().getTargetTable();
		RIGHTTABLE targetTableClone = (RIGHTTABLE) new Table(targetTable.getName());
		KeyBuilder<RIGHTTABLE, SRCID> relationOwnerForeignKey = Key.from(targetTableClone);
		((Set<Column<RIGHTTABLE, ?>>) (Set) rightKey.getColumns()).forEach(column ->
				relationOwnerForeignKey.addColumn(targetTableClone.addColumn(column.getName(), column.getJavaType()))
		);
		KeyBuilder<RIGHTTABLE, TRGTID> relationOwnerPrimaryKey = Key.from(targetTableClone);
		targetTable.getPrimaryKey().getColumns().forEach(column ->
				relationOwnerPrimaryKey.addColumn(targetTableClone.addColumn(column.getName(), column.getJavaType()))
		);
		String joinName = sourcePersister.getEntityJoinTree().addPassiveJoin(
				EntityJoinTree.ROOT_STRATEGY_NAME,
				leftKey,
				relationOwnerForeignKey.build(),
				tableAlias,
				oneToOneRelation.isNullable() ? JoinType.OUTER : JoinType.INNER,
				(Set<Column<RIGHTTABLE, ?>>) (Set) relationOwnerPrimaryKey.build().getColumns(),
				(nodeEntity, columnValueProvider) -> {
					TRGTID trgtId = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnValueProvider);
					firstPhaseCycleLoadListener.onFirstPhaseRowRead(nodeEntity, trgtId);
				},
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
