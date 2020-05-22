package org.gama.stalactite.persistence.engine.cascade;

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
import org.gama.stalactite.persistence.engine.cascade.StrategyJoins.MergeJoin;
import org.gama.stalactite.persistence.engine.cascade.StrategyJoins.RelationJoin;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.sql.result.Row;

/**
 * Tranformer of a graph of joins to a graph of entities.
 * 
 * Non Thread-safe : contains a cache of beans being loaded.
 * 
 * @param <C> type of generated beans
 * @author Guillaume Mary
 */
public class StrategyJoinsRowTransformer<C> {
	
	private final StrategyJoins<C, ?> rootStrategyJoins;
	
	/**
	 * Alias provider between strategy columns and their names in {@link java.sql.ResultSet}. Used when transforming {@link Row} to beans.
	 */
	private final ColumnedRow columnedRow;
	
	/**
	 * Constructor that links the instance to given {@link JoinedStrategiesSelect}
	 * 
	 * @param joinedStrategiesSelect the associated select maintener
	 */
	public StrategyJoinsRowTransformer(JoinedStrategiesSelect<C, ?, ?> joinedStrategiesSelect) {
		// aliases are computed on select build (done by JoinedStrategiesSelect) so we take it with a very dynamic alias provider by using
		// a Function on JoinedStrategiesSelect
		this(joinedStrategiesSelect.getJoinsRoot(), joinedStrategiesSelect::getAlias);
	}
	
	@VisibleForTesting
	protected StrategyJoinsRowTransformer(StrategyJoins<C, ?> rootStrategyJoins, Function<Column, String> aliasProvider) {
		this.rootStrategyJoins = rootStrategyJoins;
		this.columnedRow = new ColumnedRow(aliasProvider);
	}
	
	/**
	 * @return the alias provider to use to find {@link Column} values in rows of {@link #transform(Iterable, int, Map)}
	 */
	public ColumnedRow getColumnedRow() {
		return columnedRow;
	}
	
	/**
	 * 
	 * @param rows rows (coming from database select) to be read to build beans graph
	 * @param resultSize expected reuslt size, only for resulting list optimization
	 * @param entityCache used for filling relations by getting beans them from it instead of creating clones. Will be filled by newly created entity.
	 * 					  Can be used to set a bigger cache coming from a wider scope. 
	 * @return a list of root beans, built from given rows by asking internal strategy joins to instanciate and complete them
	 */
	public List<C> transform(Iterable<Row> rows, int resultSize, Map<Class, Map<Object /* identifier */, Object /* entity */>> entityCache) {
		EntityCacheWrapper entityCacheWrapper = new EntityCacheWrapper(entityCache);
		TransformerCache beanTransformerCache = new TransformerCacheWrapper(new HashMap<>());
		
		return transform(rows, resultSize, entityCacheWrapper, beanTransformerCache);
	}
	
	private List<C> transform(Iterable<Row> rows, int resultSize, EntityCacheWrapper entityCacheWrapper, TransformerCache beanTransformerCache) {
		List<C> result = new ArrayList<>(resultSize);
		for (Row row : rows) {
			Nullable<C> newInstance = transform(row, entityCacheWrapper, beanTransformerCache);
			newInstance.invoke(result::add);
		}
		return result;
	}
	
	private Nullable<C> transform(Row row, EntityCacheWrapper entityCacheWrapper, TransformerCache beanTransformerCache) {
		// Algorithm : we iterate depth by depth the tree structure of the joins
		// We start by the root of the hierarchy.
		// We process the entity of the current depth, then process the direct relations, add those relations to the depth iterator
		Nullable<C> result = Nullable.empty();
		Queue<StrategyJoins<Object, ?>> stack = new ArrayDeque<>();
		stack.add((StrategyJoins<Object, ?>) rootStrategyJoins);
		// we use a local cache of bean tranformer because we'll ask a slide of them with aliasProvider which creates an instance at each invokation
		Object rowInstance = null;
		while (!stack.isEmpty()) {
			
			// treating the current depth
			StrategyJoins<Object, ?> strategyJoins = stack.poll();
			EntityInflater<Object, Object> leftStrategy = strategyJoins.getStrategy();
			
			if (leftStrategy != null) {	// null when join is passive
				AbstractTransformer mainRowTransformer = beanTransformerCache.computeIfAbsent(leftStrategy, columnedRow);
				Object identifier = leftStrategy.giveIdentifier(row, columnedRow);
				
				rowInstance = entityCacheWrapper.computeIfAbsent(leftStrategy.getEntityType(), identifier, () -> {
					Object newInstance = mainRowTransformer.transform(row);
					if (strategyJoins == rootStrategyJoins) {
						result.elseSet((C) newInstance);
					}
					return newInstance;
				});
			}
			
			// processing the direct relations
			for (AbstractJoin join : strategyJoins.getJoins()) {
				StrategyJoins subJoins = join.getStrategy();
				if (join instanceof StrategyJoins.PassiveJoin) {
					// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
					if (!subJoins.getJoins().isEmpty()) {
						stack.add(subJoins);
					}
				} else {
					EntityInflater rightStrategy = subJoins.getStrategy();
					AbstractTransformer rowTransformer = beanTransformerCache.computeIfAbsent(rightStrategy, columnedRow);
					if (join instanceof MergeJoin) {
						rowTransformer.applyRowToBean(row, rowInstance);
						// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
						if (!subJoins.getJoins().isEmpty()) {
							stack.add(subJoins);
						}
					} else if (join instanceof RelationJoin) {
						
						Object rightIdentifier = rightStrategy.giveIdentifier(row, columnedRow);
						
						// primary key null means no entity => nothing to do
						if (rightIdentifier != null) {
							Object rightInstance = entityCacheWrapper.computeIfAbsent(rightStrategy.getEntityType(), rightIdentifier,
									() -> rowTransformer.transform(row));
							
							((RelationJoin) join).getBeanRelationFixer().apply(rowInstance, rightInstance);
							
							// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
							if (!subJoins.getJoins().isEmpty()) {
								stack.add(subJoins);
							}
						}
					} else {
						// Developer made something wrong because other types than MergeJoin and RelationJoin are not expected
						throw new IllegalArgumentException("Unexpected join type, only "
								+ Reflections.toString(MergeJoin.class) + " and " + Reflections.toString(RelationJoin.class) + " are handled"
								+ ", not " + Reflections.toString(join.getClass()));
					}
				}
			}
		}
		return result;
	}
	
	/**
	 * Simple class to ease access or creation to entity from the cache
	 * @see #computeIfAbsent(Class, Object, Supplier) 
	 */
	public static final class EntityCacheWrapper {
		
		private final Map<Class, Map<Object, Object>> entityCache;
		
		public EntityCacheWrapper(Map<Class, Map<Object, Object>> entityCache) {
			this.entityCache = entityCache;
		}
		
		/**
		 * Main method that tries to retrieve an entity by its class and identifier or instanciates it and put it into the cache
		 * 
		 * @param clazz the type of the entity
		 * @param identifier the identifier of the entity (Long, String, ...)
		 * @param factory the "method" that will be called to create the entity when the entity is not in the cache
		 * @return the existing instance in the cache or a new object
		 */
		public <C> C computeIfAbsent(Class<C> clazz, Object identifier, Supplier<C> factory) {
			Map<Object, Object> classInstanceCacheByIdentifier = entityCache.computeIfAbsent(clazz, k -> new HashMap<>());
			return (C) classInstanceCacheByIdentifier.computeIfAbsent(identifier, k -> factory.get());
		}
	}
	
	@FunctionalInterface
	public interface TransformerCache {
		
		<C> AbstractTransformer<C> computeIfAbsent(EntityInflater<C, ?> entityInflater, ColumnedRow columnedRow);
	}
	
	/**
	 * Simple class to ease access or creation to entity from the cache
	 * @see #computeIfAbsent(EntityInflater, ColumnedRow)  
	 */
	public static final class TransformerCacheWrapper implements TransformerCache {
		
		private final Map<EntityInflater, AbstractTransformer> entityCache;
		
		public TransformerCacheWrapper(Map<EntityInflater, AbstractTransformer> entityCache) {
			this.entityCache = entityCache;
		}
		
		/**
		 * Main method that tries to retrieve an entity by its class and identifier or instanciates it and put it into the cache
		 * 
		 * @param entityInflater the type of the entity
		 * @param columnedRow the identifier of the entity (Long, String, ...)
		 * @return the existing instance in the cache or a new object
		 */
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
