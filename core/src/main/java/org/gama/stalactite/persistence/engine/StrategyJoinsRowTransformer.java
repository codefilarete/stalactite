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

/**
 * @author Guillaume Mary
 */
public class StrategyJoinsRowTransformer<T> {
	
	private final StrategyJoins<?> rootStrategyJoins;
	private Map<Class, Map<Object, Object>> entityCache = new HashMap<>();
	
	public StrategyJoinsRowTransformer(StrategyJoins<?> rootStrategyJoins) {
		this.rootStrategyJoins = rootStrategyJoins;
	}
	
	public void setEntityCache(Map<Class, Map<Object, Object>> entityCache) {
		this.entityCache = entityCache;
	}
	
	public List<T> transform(Iterable<Row> rows) {
		List<T> result = new ArrayList<>();
		EntityCacheWrapper entityCacheWrapper = new EntityCacheWrapper(entityCache);
		
		for (Row row : rows) {
			Queue<StrategyJoins> stack = new ArrayDeque<>();
			stack.add(rootStrategyJoins);
			while (!stack.isEmpty()) {
				StrategyJoins<?> strategyJoins = stack.poll();
				ClassMappingStrategy<?, ?> leftStrategy = strategyJoins.getStrategy();
				ToBeanRowTransformer mainRowTransformer = leftStrategy.getRowTransformer();
				Object primaryKeyValue = row.get(strategyJoins.getTable().getPrimaryKey().getAlias());
				
				Object rowInstance = entityCacheWrapper.computeIfAbsent(leftStrategy.getClassToPersist(), primaryKeyValue, () -> {
					Object newInstance = mainRowTransformer.newRowInstance();
					mainRowTransformer.applyRowToBean(row, newInstance);
					if (strategyJoins == rootStrategyJoins) {
						result.add((T) newInstance);
					}
					return newInstance;
				});
				
				for (Join join : strategyJoins.getJoins()) {
					StrategyJoins rightMember = join.getStrategy();
					Object primaryKeyValue2 = row.get(rightMember.getTable().getPrimaryKey().getAlias());
					
					ToBeanRowTransformer rowTransformer = rightMember.getStrategy().getRowTransformer();
					Object rowInstance2 = entityCacheWrapper.computeIfAbsent(rightMember.getStrategy().getClassToPersist(), primaryKeyValue2, () -> {
						Object newInstance = rowTransformer.newRowInstance();
						rowTransformer.applyRowToBean(row, newInstance);
						return newInstance;
					});
					
					// TODO: generic: la signature en Iterable du join.getSetter est fausse en OneToOne, je me demande comment ça marche ! il faudra sans doute mettre "Object" au lieu de Iterable
					// TODO: implémenter l'héritage d'entité
					// TODO: implémenter ManyToMany ?
					fillRelation(rowInstance, rowInstance2, join.getSetter(), join.getGetter(), join.getOneToManyType());
					stack.add(rightMember);
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
