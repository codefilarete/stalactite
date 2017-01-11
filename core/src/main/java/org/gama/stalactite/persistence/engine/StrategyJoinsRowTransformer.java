package org.gama.stalactite.persistence.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.lang.Reflections;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ToBeanRowTransformer;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class StrategyJoinsRowTransformer<T> {
	
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
	 * Change the alias provider (default is {@link Column#getAlias()}) by giving the map between {@link Column} and their alias.
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
			while (!stack.isEmpty()) {
				
				// treating the current depth
				StrategyJoins<?> strategyJoins = stack.poll();
				ClassMappingStrategy<?, ?> leftStrategy = strategyJoins.getStrategy();
				ToBeanRowTransformer mainRowTransformer = leftStrategy.getRowTransformer();
				Object primaryKeyValue = row.get(getAlias(strategyJoins.getTable().getPrimaryKey()));
				
				Object rowInstance = entityCacheWrapper.computeIfAbsent(leftStrategy.getClassToPersist(), primaryKeyValue, () -> {
					Object newInstance = mainRowTransformer.newRowInstance();
					mainRowTransformer.withAliases(aliasProvider).applyRowToBean(row, newInstance);
					if (strategyJoins == rootStrategyJoins) {
						result.add((T) newInstance);
					}
					return newInstance;
				});
				
				// processing the direct relations
				for (Join join : strategyJoins.getJoins()) {
					StrategyJoins rightMember = join.getStrategy();
					Object primaryKeyValue2 = row.get(getAlias(rightMember.getTable().getPrimaryKey()));
					
					ToBeanRowTransformer rowTransformer = rightMember.getStrategy().getRowTransformer();
					Object rowInstance2 = entityCacheWrapper.computeIfAbsent(rightMember.getStrategy().getClassToPersist(), primaryKeyValue2, () -> {
						Object newInstance = rowTransformer.newRowInstance();
						rowTransformer.withAliases(aliasProvider).applyRowToBean(row, newInstance);
						return newInstance;
					});
					
					// TODO: implémenter l'héritage d'entité
					// TODO: implémenter ManyToMany ?
					fillRelation(rowInstance, rowInstance2, join.getSetter(), join.getGetter(), join.getOneToManyType());
					
					// don't forget to add the member to the depth iteration
					if (!rightMember.getJoins().isEmpty()) {
						stack.add(rightMember);
					}
				}
			}
		}
		return result;
	}
	
	private <K, V> void fillRelation(K rowInstance, V rowInstance2, BiConsumer setter, Function getter, Class oneToManyType) {
		if (oneToManyType != null) {
			fillOneToMany(rowInstance, rowInstance2, (BiConsumer<K, Collection<V>>) setter, (Function<K, Collection<V>>) getter, (Class<Collection<V>>) oneToManyType);
		} else {
			setter.accept(rowInstance, rowInstance2);
		}
	}
	
	private <K, V> void fillOneToMany(K rowInstance, V rowInstance2, BiConsumer<K, Collection<V>> setter, Function<K, Collection<V>> getter, Class<Collection<V>> oneToManyType) {
		Collection<V> oneToManyTarget = getter.apply(rowInstance);
		// if the container doesn't exist, we create and fix it
		if (oneToManyTarget == null) {
			oneToManyTarget = Reflections.newInstance(oneToManyType);
			setter.accept(rowInstance, oneToManyTarget);
		}
		oneToManyTarget.add(rowInstance2);
	}
	
	/**
	 * Simple class to ease access or creation to entity from the cache
	 * @see #computeIfAbsent(Class, Object, Supplier) 
	 */
	private final class EntityCacheWrapper {
		
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
		public Object computeIfAbsent(Class clazz, Object identifier, Supplier<Object> factory) {
			Object rowInstance = entityCache.computeIfAbsent(clazz, k -> new HashMap<>()).get(identifier);
			if (rowInstance == null) {
				rowInstance = factory.get();
				entityCache.computeIfAbsent(clazz, k -> new HashMap<>()).put(identifier, rowInstance);
			}
			return rowInstance;
		}
	}
}
