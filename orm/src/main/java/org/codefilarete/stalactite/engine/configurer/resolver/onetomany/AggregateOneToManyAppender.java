package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateOneToManyAppender {
	
	private final OneToManyResolver oneToManyResolver;
	
	public AggregateOneToManyAppender(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.oneToManyResolver = new OneToManyResolver(skeletonAggregateResolver, dialect, connectionConfiguration);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	void append(Entity<SRC, SRCID, LEFTTABLE> rootEntity, ConfiguredRelationalPersister<SRC, SRCID> rootPersister) {
		
		// Iterating over all the one-to-many relations of the tree (starting from given root entity).
		// It's made by a breadth-first algorithm with node stacking, no recursion here.
		// Bread-first principle shouldn't be important because we maintain some AssemblyPoints to keep track of the
		// depth and the necessary information for the next iteration.
		Queue<AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE>> relationStack = new ArrayDeque<>();
		// We start by a kind of fake seed, without relation, because we don't have any for the root entity
		relationStack.add(new AssemblyPoint<>(rootEntity, rootPersister, ROOT_JOIN_NAME, null));
		
		while (!relationStack.isEmpty()) {
			AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn = relationStack.poll();
			assemblyPawn.getRelationOwnerEntity().getRelations().stream()
					.filter(ResolvedOneToManyRelation.class::isInstance)
					.map(ResolvedOneToManyRelation.class::cast)
					.forEach(relationPawn -> {
								ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation = relationPawn;
								oneToManyResolver.resolve(
										relation,
										assemblyPawn.getRelationOwnerPersister(),
										targetPersister -> {
											
											PropertyAccessor<SRC, S> accessor;
											if (assemblyPawn.getParentJoinPoint().equals(ROOT_JOIN_NAME)) {
												// this is the very first step (see stack seed) which is the root entity, no relation accessor shifting here
												accessor = relation.getAccessor();
											} else {
												// we need to shift the relation accessor by the parent accessor
												AccessorChain<SRC, S> shifter = new AccessorChain<>(assemblyPawn.getAccessor(), relation.getAccessor());
												shifter.setNullValueHandler(AccessorChain.RETURN_NULL);
												accessor = shifter;
											}
											
											if (relation.isOwnedByReverseSide()) {
												Set<Column<RIGHTTABLE, ?>> columnsToSelect;
												Function<ColumnedRow, Object> duplicateIdentifierProvider;
												if (relation.isOrdered()) {
													columnsToSelect = new HashSet<>(targetPersister.<RIGHTTABLE>getMainTable().getPrimaryKey().getColumns());
													columnsToSelect.add(relation.getIndexingMappedColumn());
													duplicateIdentifierProvider = (columnedRow) -> {
														TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
														Integer targetEntityIndex = columnedRow.get(relation.getIndexingMappedColumn());
														return identifier + "-" + targetEntityIndex;
													};
												} else {
													columnsToSelect = Collections.emptySet();
													duplicateIdentifierProvider = (columnedRow) -> {
														TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
														return identifier;
													};
												}
												DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join = (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID>) relation.getJoin();
												String joinName = targetPersister.joinAsMany(
														assemblyPawn.getParentJoinPoint(),
														rootPersister,
														accessor,
														join.getLeftKey(),
														join.getRightKey(),
														relation.getRelationFixer(),
														duplicateIdentifierProvider,
														columnsToSelect,
														true,
														relation.isFetchSeparately());
												
												// Preparing for next iteration
												// Note that we can't set the correct generics types to the AssemblyPoint instance
												// because we go a step further in the relation by shifting the types from SRC to TRGT 
												relationStack.add(new AssemblyPoint(relation.getTargetEntity(), targetPersister, joinName, accessor));
											} else {
												String manyJoinName;
												if (relation.isOrdered()) {
													manyJoinName = appendIndexedAssociation(rootPersister, targetPersister, relation, assemblyPawn, accessor);
												} else {
													manyJoinName = appendAssociation(rootPersister, targetPersister, relation, assemblyPawn, accessor);
												}
												
												// Preparing for next iteration
												// Note that we can't set the correct generics types to the AssemblyPoint instance
												// because we go a step further in the relation by shifting the types from SRC to TRGT 
												relationStack.add(new AssemblyPoint(relation.getTargetEntity(), targetPersister, manyJoinName, accessor));
											}
											
											SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
											assemblyPawn.getRelationOwnerPersister().addSelectListener(new SelectListener<SRC, SRCID>() {
												@Override
												public void beforeSelect(Iterable<SRCID> ids) {
													// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
													targetSelectListener.beforeSelect(Collections.emptyList());
												}
												
												@Override
												public void afterSelect(Set<? extends SRC> result) {
													Set<TRGT> collect = Iterables.stream(result).flatMap(src -> Nullable.nullable(relation.getAccessor().get(src))
																	.map(Collection::stream)
																	.getOr(Stream.empty()))
															.collect(Collectors.toSet());
													targetSelectListener.afterSelect(collect);
												}
												
												@Override
												public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
													// since ids are not those of its entities, we should not pass them as argument
													targetSelectListener.onSelectError(Collections.emptyList(), exception);
												}
											});
										});
							}
					);
		}
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	String appendAssociation(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                                ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
	                                ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn,
	                                PropertyAccessor<SRC, S> accessor) {
		Function<ColumnedRow, Object> duplicateIdentifierProvider = null;
		Set<Column<ASSOCIATIONTABLE, Integer>> columnsToSelect = Collections.emptySet();
		
		// we join on the association table
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		String associationTableJoinName = rootPersister.getEntityJoinTree().addPassiveJoin(
				assemblyPawn.getParentJoinPoint(),
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				columnsToSelect);
		
		return rootPersister.getEntityJoinTree().addRelationJoin(
				associationTableJoinName,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.<RIGHTTABLE>getMapping()),
				accessor,
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				relation.getRelationFixer(),
				Collections.emptySet(),
				duplicateIdentifierProvider);
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	String appendIndexedAssociation(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                         ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
	                         ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                         AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn,
	                         PropertyAccessor<SRC, S> accessor) {
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		Holder<String> associationTableJoinNodeNameHolder = new Holder<>();
		Column<ASSOCIATIONTABLE, Integer> indexingColumn = (Column<ASSOCIATIONTABLE, Integer>) relation.<IndexedAssociationTable>getIndexingAssociationColumn();
		Function<ColumnedRow, Object> duplicateIdentifierProvider = (columnedRow) -> {
			TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
			// indexColumn column value is took on join of association table, not target table, so we have to grab it
			JoinNode<IndexedAssociationRecord, Fromable> joinNode = (JoinNode<IndexedAssociationRecord, Fromable>) rootPersister.getEntityJoinTree().getJoin(associationTableJoinNodeNameHolder.get());
			ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(joinNode);
			Integer targetEntityIndex = rowDecoder.get(indexingColumn);
			return identifier + "-" + targetEntityIndex;
		};
		
		// we join on the association table
		String associationTableJoinName = rootPersister.getEntityJoinTree().addPassiveJoin(
				assemblyPawn.getParentJoinPoint(),
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				// we must add all the columns to make them available while decoding the row to create an IndexedAssociationRecord
				join.getJoinTable().getColumns());
		associationTableJoinNodeNameHolder.set(associationTableJoinName);

		// Implementation note: we keep the object indexes and put the sorted entities in a temporary Collection, then add them all to the target List
		InMemoryRelationHolder<SRC, SRCID, TRGT, S> inMemoryRelationFixer = new InMemoryRelationHolder<>(
				assemblyPawn.getRelationOwnerPersister()::getId,
				relation.getAccessor(),
				relation.getComponentFactory(),
				relation.getMappedByAccessor()
		);
		rootPersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationFixer.init();
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				inMemoryRelationFixer.applySort(result);
				cleanContext();
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				inMemoryRelationFixer.clear();
			}
		});
		
		String manyJoinName = rootPersister.getEntityJoinTree().addRelationJoin(
				associationTableJoinName,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.<RIGHTTABLE>getMapping()),
				accessor,
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				inMemoryRelationFixer,
				Collections.emptySet(),
				duplicateIdentifierProvider);
		
		JoinNode<TRGT, Fromable> joinNode = (JoinNode<TRGT, Fromable>) rootPersister.getEntityJoinTree().getJoin(manyJoinName);
		JoinNode<?, Fromable> associationJoinNode = rootPersister.getEntityJoinTree().getJoin(associationTableJoinName);
		IndexedAssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping
				= new IndexedAssociationRecordMapping<>(
				join.getJoinTable(),
				assemblyPawn.getRelationOwnerPersister().getMapping().getIdMapping().getIdentifierAssembler(),
				targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
				join.getJoinTable().getLeftIdentifierColumnMapping(),
				join.getJoinTable().getRightIdentifierColumnMapping());
		joinNode.setConsumptionListener((trgt, columnValueProvider) -> {
			ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(associationJoinNode);
			IndexedAssociationRecord associationRecord = associationRecordMapping.getRowTransformer().transform(rowDecoder);
			inMemoryRelationFixer.addIndex((SRCID) associationRecord.getLeft(), trgt, associationRecord.getIndex());
		});

		return manyJoinName;
	}
	
	private static class AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>> {
		
		private final Entity<SRC, SRCID, LEFTTABLE> relationOwnerEntity;
		private final ConfiguredRelationalPersister<SRC, SRCID> relationOwnerPersister;
		private final String parentJoinPoint;
		private final PropertyAccessor<SRC, TRGT> accessor;
		
		private AssemblyPoint(Entity<SRC, SRCID, LEFTTABLE> relationOwnerEntity,
		                      ConfiguredRelationalPersister<SRC, SRCID> relationOwnerPersister,
		                      String parentJoinPoint,
		                      PropertyAccessor<SRC, TRGT> accessor) {
			this.relationOwnerEntity = relationOwnerEntity;
			this.relationOwnerPersister = relationOwnerPersister;
			this.parentJoinPoint = parentJoinPoint;
			this.accessor = accessor;
		}
		
		public ConfiguredRelationalPersister<SRC, SRCID> getRelationOwnerPersister() {
			return relationOwnerPersister;
		}
		
		public String getParentJoinPoint() {
			return parentJoinPoint;
		}
		
		public Entity<SRC, SRCID, LEFTTABLE> getRelationOwnerEntity() {
			return relationOwnerEntity;
		}
		
		public PropertyAccessor<SRC, TRGT> getAccessor() {
			return accessor;
		}
	}
}
