package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTree.MergeJoin;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTree.PassiveJoin;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTree.RelationJoin;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.sql.result.Row;

/**
 * Tranformer of a graph of joins to a graph of entities.
 * 
 * @param <C> type of generated beans
 * @author Guillaume Mary
 */
public class EntityMappingStrategyTreeRowTransformer<C> {
	
	public static final ThreadLocal<EntityCache> CURRENT_ENTITY_CACHE = new ThreadLocal<>();
	
	public static final ThreadLocal<TransformerCache> TRANSFORMER_CACHE = new ThreadLocal<>();
	
	private final EntityMappingStrategyTree<C, ?> rootEntityMappingStrategyTree;
	
	/**
	 * Alias provider between strategy columns and their names in {@link java.sql.ResultSet}. Used when transforming {@link Row} to beans.
	 */
	private final ColumnedRow columnedRow;
	
	/**
	 * Constructor that links the instance to given {@link EntityMappingStrategyTreeSelectBuilder}
	 * 
	 * @param entityMappingStrategyTreeSelectBuilder the associated select maintener
	 */
	public EntityMappingStrategyTreeRowTransformer(EntityMappingStrategyTreeSelectBuilder<C, ?, ?> entityMappingStrategyTreeSelectBuilder) {
		// aliases are computed on select build (done by EntityMappingStrategyTreeSelectBuilder) so we take it with a very dynamic alias provider by using
		// a Function on EntityMappingStrategyTreeSelectBuilder
		this(entityMappingStrategyTreeSelectBuilder.getRoot(), entityMappingStrategyTreeSelectBuilder::getAlias);
	}
	
	@VisibleForTesting
	protected EntityMappingStrategyTreeRowTransformer(EntityMappingStrategyTree<C, ?> rootEntityMappingStrategyTree, Function<Column, String> aliasProvider) {
		this.rootEntityMappingStrategyTree = rootEntityMappingStrategyTree;
		this.columnedRow = new ColumnedRow(aliasProvider);
	}
	
	/**
	 * @return the alias provider to find {@link Column} values in rows of {@link #transform(Iterable, int)}
	 */
	public ColumnedRow getColumnedRow() {
		return columnedRow;
	}
	
	/**
	 * 
	 * @param rows rows (coming from database select) to be read to build beans graph
	 * @param resultSize expected reuslt size, only for resulting list optimization
	 * @return a list of root beans, built from given rows by asking internal strategy joins to instanciate and complete them
	 */
	public List<C> transform(Iterable<Row> rows, int resultSize) {
		return ThreadLocals.doWithThreadLocal(CURRENT_ENTITY_CACHE, BasicEntityCache::new, (Function<EntityCache, List<C>>) entityCache ->
			ThreadLocals.doWithThreadLocal(TRANSFORMER_CACHE, BasicTransformerCache::new, (Function<TransformerCache, List<C>>) transformerCache ->
				transform(rows, resultSize, entityCache, transformerCache)
			)
		);
	}
	
	private List<C> transform(Iterable<Row> rows, int resultSize, EntityCache entityCache, TransformerCache transformerCache) {
		List<C> result = new ArrayList<>(resultSize);
		for (Row row : rows) {
			Nullable<C> newInstance = transform(row, entityCache, transformerCache);
			newInstance.invoke(result::add);
		}
		return result;
	}
	
	private Nullable<C> transform(Row row, EntityCache entityCache, TransformerCache beanTransformerCache) {
		// Algorithm : we iterate depth by depth the tree structure of the joins
		// We start by the root of the hierarchy.
		// We process the entity of the current depth, then process the direct relations, add those relations to the depth iterator
		Nullable<C> result = Nullable.empty();
		Queue<EntityMappingStrategyTree<Object, ?>> stack = new ArrayDeque<>();
		stack.add((EntityMappingStrategyTree<Object, ?>) rootEntityMappingStrategyTree);
		// we use a local cache of bean tranformer because we'll ask a slide of them with aliasProvider which creates an instance at each invokation
		Object rowInstance = null;
		while (!stack.isEmpty()) {
			
			// treating the current depth
			EntityMappingStrategyTree<Object, ?> currentMainStrategy = stack.poll();
			
			if (currentMainStrategy.getStrategy() != null) {	// null when join is passive
				rowInstance = giveRowInstance(row, currentMainStrategy, entityCache, beanTransformerCache, result);
			}
			
			// processing the direct relations
			for (EntityMappingStrategyTreeJoinPoint join : currentMainStrategy.getJoins()) {
				EntityMappingStrategyTree subJoins = join.getStrategy();
				if (join instanceof PassiveJoin) {
					// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
					addJoinsToStack(subJoins, stack);
				} else if (join instanceof MergeJoin) {
					AbstractTransformer rowTransformer = beanTransformerCache.computeIfAbsent(subJoins.getStrategy(), columnedRow);
					rowTransformer.applyRowToBean(row, rowInstance);
					// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
					addJoinsToStack(subJoins, stack);
				} else if (join instanceof RelationJoin) {
					boolean relationApplied = applyRelatedBean(row, rowInstance, ((RelationJoin) join).getBeanRelationFixer(),
							subJoins.getStrategy(),
							entityCache, beanTransformerCache);
					// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
					if (relationApplied) {
						addJoinsToStack(subJoins, stack);
					}
				} else {
					// Developer made something wrong because other types than MergeJoin and RelationJoin are not expected
					throw new IllegalArgumentException("Unexpected join type, only "
							+ Reflections.toString(MergeJoin.class) + " and " + Reflections.toString(RelationJoin.class) + " are handled"
							+ ", not " + Reflections.toString(join.getClass()));
				}
			}
		}
		return result;
	}
	
	private void addJoinsToStack(EntityMappingStrategyTree joins, Queue<EntityMappingStrategyTree<Object, ?>> stack) {
		if (!joins.getJoins().isEmpty()) {
			stack.add(joins);
		}
	}
	
	private Object giveRowInstance(Row row,
								   EntityMappingStrategyTree<Object, ?> entityMappingStrategyTree,
								   EntityCache entityCache,
								   TransformerCache beanTransformerCache,
								   Nullable<C> rootBeanHolder) {
		EntityInflater<Object, Object> entityInflater = entityMappingStrategyTree.getStrategy();
		AbstractTransformer rowTransformer = beanTransformerCache.computeIfAbsent(entityInflater, columnedRow);
		Object identifier = entityInflater.giveIdentifier(row, columnedRow);
		return entityCache.computeIfAbsent(entityInflater.getEntityType(), identifier, () -> {
				Object newInstance = rowTransformer.transform(row);
				if (entityMappingStrategyTree == rootEntityMappingStrategyTree) {
					rootBeanHolder.elseSet((C) newInstance);
				}
				return newInstance;
			});
	}
	
	private boolean applyRelatedBean(Row row,
									 Object rowInstance,
									 BeanRelationFixer beanRelationFixer,
									 EntityInflater entityInflater,
									 EntityCache entityCache,
									 TransformerCache beanTransformerCache) {
		Object rightIdentifier = entityInflater.giveIdentifier(row, columnedRow);
		// primary key null means no entity => nothing to do
		if (rightIdentifier != null) {
			AbstractTransformer rowTransformer = beanTransformerCache.computeIfAbsent(entityInflater, columnedRow);
			Object rightInstance = entityCache.computeIfAbsent(entityInflater.getEntityType(), rightIdentifier,
					() -> rowTransformer.transform(row));
			
			beanRelationFixer.apply(rowInstance, rightInstance);
			return true;
		}
		return false;
	}
	
	@FunctionalInterface
	public interface EntityCache {
		
		/**
		 * Expected to retrieve an entity by its class and identifier from cache or instanciates it and put it into the cache
		 *
		 * @param clazz the type of the entity
		 * @param identifier the identifier of the entity (Long, String, ...)
		 * @param factory the "method" that will be called to create the entity when the entity is not in the cache
		 * @return the existing instance in the cache or a new object
		 */
		<C> C computeIfAbsent(Class<C> clazz, Object identifier, Supplier<C> factory);
		
	}
	
	/**
	 * Simple class to ease access or creation to entity from the cache
	 * @see #computeIfAbsent(Class, Object, Supplier) 
	 */
	private static final class BasicEntityCache implements EntityCache {
		
		private final Map<Class, Map<Object, Object>> entityCache = new HashMap<>();
		
		public <C> C computeIfAbsent(Class<C> clazz, Object identifier, Supplier<C> factory) {
			Map<Object, Object> classInstanceCacheByIdentifier = entityCache.computeIfAbsent(clazz, k -> new HashMap<>());
			return (C) classInstanceCacheByIdentifier.computeIfAbsent(identifier, k -> factory.get());
		}
	}
	
	@FunctionalInterface
	public interface TransformerCache {
		
		/**
		 * Expected to retrieve an {@link AbstractTransformer} by its {@link EntityInflater} from cache or instanciates it and put it into the cache
		 *
		 * @param entityInflater the {@link EntityInflater} owning {@link AbstractTransformer}
		 * @param columnedRow the necessary {@link ColumnedRow} that contains aliases to be used by resulting {@link AbstractTransformer}
		 * @return the existing instance in the cache or a new object
		 */
		<C> AbstractTransformer<C> computeIfAbsent(EntityInflater<C, ?> entityInflater, ColumnedRow columnedRow);
	}
	
	/**
	 * Simple class to ease access or creation to entity from the cache
	 * @see #computeIfAbsent(EntityInflater, ColumnedRow)  
	 */
	private static final class BasicTransformerCache implements TransformerCache {
		
		private final Map<EntityInflater, AbstractTransformer> entityCache = new HashMap<>();
		
		public <C> AbstractTransformer<C> computeIfAbsent(EntityInflater<C, ?> entityInflater, ColumnedRow columnedRow) {
			return entityCache.computeIfAbsent(entityInflater, inflater -> inflater.copyTransformerWithAliases(columnedRow));
		}
	}
	
	/**
	 * Constract to deserialize a database row to a bean
	 * 
	 * @param <E>
	 * @param <I>
	 */
	public interface EntityInflater<E, I> {
		
		Class<E> getEntityType();
		
		I giveIdentifier(Row row, ColumnedRow columnedRow);
		
		AbstractTransformer copyTransformerWithAliases(ColumnedRow columnedRow);
		
	}
}
