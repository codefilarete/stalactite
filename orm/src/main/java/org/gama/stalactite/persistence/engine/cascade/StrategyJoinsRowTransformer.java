package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @param <T> type of generated beans
 * @author Guillaume Mary
 */
public class StrategyJoinsRowTransformer<T> {
	
	/**
	 * Default alias provider between strategy columns and their names in {@link java.sql.ResultSet}
	 * Used when transforming {@link Row} to beans.
	 * By default it is {@link Column#getAlias()} (which is hardly used at runtime because of joined columns naming strategy)
	 */
	public static final Function<Column, String> DEFAULT_ALIAS_PROVIDER = Column::getAlias;
	
	private final StrategyJoins<?> rootStrategyJoins;
	private Map<Class, Map<Object /* identifier */, Object /* entity */>> entityCache = new HashMap<>();
	private Function<Column, String> aliasProvider = DEFAULT_ALIAS_PROVIDER;
	
	public StrategyJoinsRowTransformer(StrategyJoins<?> rootStrategyJoins) {
		this.rootStrategyJoins = rootStrategyJoins;
	}
	
	/**
	 * Give an entity pool (cache ?) to this instance, to be used for filling relations by getting them from it instead of creating clones.
	 * Will be filled by newly created entity.
	 * 
	 * @param entityCache the Maps of entities per identifier, mapped by Class persistence 
	 */
	public void setEntityCache(Map<Class, Map<Object /* identifier */, Object /* entity */>> entityCache) {
		this.entityCache = entityCache;
	}
	
	/**
	 * 
	 * @return
	 */
	public Function<Column, String> getAliasProvider() {
		return aliasProvider;
	}
	
	/**
	 * Changes the alias provider (default is {@link Column#getAlias()}) by giving the map between {@link Column} and their alias.
	 * @param aliases the mapping between {@link Column} and their alias in the Rows of {@link #transform(Iterable)}
	 */
	public void setAliases(Map<Column, String> aliases) {
		this.aliasProvider = aliases::get;
	}
	
	private String getAlias(Column primaryKey) {
		return aliasProvider.apply(primaryKey);
	}
	
	public List<T> transform(Iterable<Row> rows) {
		List<T> result = new ArrayList<>();
		EntityCacheWrapper entityCacheWrapper = new EntityCacheWrapper(entityCache);
		
		for (Row row : rows) {
			// Algorithm : we iterate depth by depth the tree structure of the joins
			// We start by the root of the hierarchy.
			// We process the entity of the current depth, then process the direct relations, add those relations to the depth iterator
			
			
			Queue<StrategyJoins> stack = new ArrayDeque<>();
			stack.add(rootStrategyJoins);
			// we use a local cache of bean tranformer because we'll ask a slide of them with aliasProvider which creates an instance at each invokation
			Map<ClassMappingStrategy, ToBeanRowTransformer> beanTransformerCache = new HashMap<>();
			while (!stack.isEmpty()) {
				
				// treating the current depth
				StrategyJoins<Object> strategyJoins = stack.poll();
				ClassMappingStrategy<Object, Object, Table> leftStrategy = strategyJoins.getStrategy();
				ToBeanRowTransformer mainRowTransformer = leftStrategy.getRowTransformer();
				Object primaryKeyValue = row.get(getAlias(strategyJoins.getTable().getPrimaryKey()));
				
				Object rowInstance = entityCacheWrapper.computeIfAbsent(leftStrategy.getClassToPersist(), primaryKeyValue, () -> {
					Object newInstance = mainRowTransformer.newBeanInstance();
					mainRowTransformer.withAliases(aliasProvider).applyRowToBean(row, newInstance);
					if (strategyJoins == rootStrategyJoins) {
						result.add((T) newInstance);
					}
					return newInstance;
				});
				
				// processing the direct relations
				for (Join join : strategyJoins.getJoins()) {
					StrategyJoins rightStrategy = join.getStrategy();
					Object rightPrimaryKeyValue = row.get(getAlias(rightStrategy.getTable().getPrimaryKey()));
					
					// primary key null means no entity => nothing to do
					if (rightPrimaryKeyValue != null) {
						ToBeanRowTransformer rowTransformer = beanTransformerCache.computeIfAbsent(rightStrategy.getStrategy(), s -> s.getRowTransformer().withAliases(aliasProvider));
						Object rightInstance = entityCacheWrapper.computeIfAbsent(rightStrategy.getStrategy().getClassToPersist(), rightPrimaryKeyValue,
								() -> rowTransformer.transform(row));
						
						// TODO: implémenter l'héritage d'entité
						// TODO: implémenter ManyToMany ?
						join.getBeanRelationFixer().apply(rowInstance, rightInstance);
						
						// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
						if (!rightStrategy.getJoins().isEmpty()) {
							stack.add(rightStrategy);
						}
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
	private static final class EntityCacheWrapper {
		
		private final Map<Class, Map<Object, Object>> entityCache;
		
		private EntityCacheWrapper(Map<Class, Map<Object, Object>> entityCache) {
			this.entityCache = entityCache;
		}
		
		/**
		 * Main class that tries to retrieve an entity by its class and identifier or instanciates it and put it into the cache
		 * 
		 * @param clazz the type of the entity
		 * @param identifier the identifier of the entity (Long, String, ...)
		 * @param factory the "method" that will be called to create the entity when the entity is not in the cache
		 * @return the existing instance in the cache or a new object
		 */
		public <C> C computeIfAbsent(Class<C> clazz, Object identifier, Supplier<C> factory) {
			Map<Object, Object> classInstanceCacheByIdentifier = entityCache.computeIfAbsent(clazz, k -> new HashMap<>());
			C rowInstance = (C) classInstanceCacheByIdentifier.get(identifier);
			if (rowInstance == null) {
				rowInstance = factory.get();
				classInstanceCacheByIdentifier.put(identifier, rowInstance);
			}
			return rowInstance;
		}
	}
}
