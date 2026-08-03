package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassPolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismWriter;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.MutableInt;

public class TablePerClassAppender {
	
	public static final String ENTITY_TYPE_DISCRIMINATOR_NAME = "clazz_";
	
	public TablePerClassAppender() {
	}
	
	public <SRC, SRCID, TRGT, TRGTID, SUBTRGT extends TRGT, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	GraftPoint append(EntityJoinTree<SRC, SRCID> aggregateTree,
	                  EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  TablePerClassPolymorphismWriter<TRGT, TRGTID, LEFTTABLE, SUBTRGT> targetPersister,
	                  TablePerClassPolymorphism<SRC, SRCID> polymorphisn,
					  ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relationPawn,
	                  String mountPoint) {
		
		Holder<GraftPoint> resultHolder = new Holder<>();
//		tablePerClassResolver.<TRGT, TRGTID, RIGHTTABLE, SUBTRGT>resolve(relationPawn.getTargetEntity(), targetPersister -> {
		Map<Class<SUBTRGT>, EntityReader<SUBTRGT, TRGTID, ?>> subEntitiesPersisters = null;// = targetPersister.getSubEntitiesPersisters();
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join = relationPawn.getJoin();
		TablePerClassUnion union = buildUnion(
				subEntitiesPersisters.values(),
				join.getRightKey());
		
		// accessor shifting — identical to AggregateOneToOneAppender lines 37-46
		PropertyAccessor<SRC, TRGT> accessor = relationPawn.getAccessor();
//			PropertyAccessor<SRC, TRGT> accessor = shift(assemblyPawn, relationPawn.getAccessor());

//			Holder<TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID>> nodeHolder = new Holder<>();
		String joinName = aggregateTree.<LEFTTABLE>addJoin(
				mountPoint,
				parent -> {
//					return null;
					TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID> node =
							new TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID>(
									(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
									union,
									accessor,
									join.getLeftKey(),
									join.getRightKey(),
									EntityJoinTree.JoinType.OUTER,
									union.getColumns(),
									targetPersister.getClassToPersist().getSimpleName(),
									new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
									(BeanRelationFixer<Object, TRGT>) relationPawn.getRelationFixer(),
									union.getDiscriminatorColumn());
//					nodeHolder.set(node);
					return node;
				});
		
		// attach the per-sub-persister merge joins (legacy addTablePerClassPolymorphicSubPersistersJoins, lines 439-468)
//			attachSubPersisterJoins(rootPersister.getEntityJoinTree(), joinName, nodeHolder.get(), union);
		
		// descend further: sub-entities' OWN relations are appended by the existing BFS
//		resultHolder.set(new GraftPoint(relationPawn.getTargetEntity(), targetPersister, joinName, aggregateTree));
//		});
		return resultHolder.get();
	}
	
	private <TRGT, TRGTID, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT, JOINTYPE>
	TablePerClassUnion<SUBTRGT, TRGTID> buildUnion(Iterable<EntityReader<SUBTRGT, TRGTID, ?>> subPersisters,
	                                               Key<RIGHTTABLE, JOINTYPE> rightJoinColumn) {
		// Union will contain only 3 columns :
		// - discriminator
		// - entity primary key
		// - join column
		TablePerClassUnion<SUBTRGT, TRGTID> result = new TablePerClassUnion<>(ENTITY_TYPE_DISCRIMINATOR_NAME);
		
		PrimaryKey<RIGHTTABLE, TRGTID> primaryKey = rightJoinColumn.getTable().getPrimaryKey();
		// adding the pseudo columns for the primary key of the main entity to create the entity identifier
		primaryKey.getColumns().forEach(column -> {
			result.registerColumn(column.getExpression(), column.getJavaType(), column.getExpression());
		});
		
		MutableInt discriminatorComputer = new MutableInt();
		
		subPersisters.forEach(subPersister -> {
			Query subEntityQuery = new Query(subPersister.getMapping().getTargetTable());
			int discriminatorValue = discriminatorComputer.increment();
			result.getSubtypeSelectorPerDiscriminatorValue().put(discriminatorValue, subPersister);
			subEntityQuery.select(String.valueOf(discriminatorValue), Integer.class, ENTITY_TYPE_DISCRIMINATOR_NAME);
			result.unionAll(subEntityQuery);
			
			rightJoinColumn.getColumns().forEach(column -> {
				subEntityQuery.select(column.getExpression(), column.getJavaType());
			});
			
			// we add sub primary key columns to make them available to the union, then they can be used to create the entity identifier
			// through idMapping.getIdentifierAssembler().getColumns()
			primaryKey.getColumns().forEach(column -> {
				subEntityQuery.select(column.getName(), column.getJavaType());
			});
		});
		
		// adding the join columns as being selectable in the union
		rightJoinColumn.getColumns().forEach(column -> {
			result.registerColumn(column.getExpression(), column.getJavaType(), column.getExpression());
		});
		return result;
	}
	
	private static class TablePerClassUnion<SUBTRGT, TRGTID> extends Union {
		
		private final PseudoColumn<Integer> discriminatorColumn;
		
		private final Map<Integer, SelectExecutor<SUBTRGT, TRGTID>> subtypeSelectorPerDiscriminatorValue = new HashMap<>();
		
		private TablePerClassUnion(String discriminatorColumnName) {
			this.discriminatorColumn = registerColumn(discriminatorColumnName, Integer.class);
		}
		
		public PseudoColumn<Integer> getDiscriminatorColumn() {
			return discriminatorColumn;
		}
		
		public Map<Integer, SelectExecutor<SUBTRGT, TRGTID>> getSubtypeSelectorPerDiscriminatorValue() {
			return subtypeSelectorPerDiscriminatorValue;
		}
	}
}
