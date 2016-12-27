package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Supplier;

import org.gama.lang.Reflections;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.JoinedStrategiesSelect.StrategyJoins.Join;
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
				ToBeanRowTransformer mainRowTransformer = strategyJoins.getStrategy().getRowTransformer();
				Object primaryKeyValue = row.get(strategyJoins.getTable().getPrimaryKey().getAlias());
				
				Object rowInstance = entityCacheWrapper.computeIfAbsent(strategyJoins.getStrategy().getClassToPersist(), primaryKeyValue, () -> {
					Object newInstance = mainRowTransformer.newRowInstance();
					mainRowTransformer.applyRowToBean(row, newInstance);
					if (strategyJoins == rootStrategyJoins) {
						result.add((T) newInstance);
					}
					return newInstance;
				});
				
				for (Join join : strategyJoins.getJoins()) {
					Object primaryKeyValue2 = row.get(join.getStrategy().getTable().getPrimaryKey().getAlias());
					
					ToBeanRowTransformer rowTransformer = join.getStrategy().getStrategy().getRowTransformer();
					Object rowInstance2 = entityCacheWrapper.computeIfAbsent(join.getStrategy().getStrategy().getClassToPersist(), primaryKeyValue2, () -> {
						Object newInstance = rowTransformer.newRowInstance();
						rowTransformer.applyRowToBean(row, newInstance);
						return newInstance;
					});
					
					Method capture = new MethodReferenceCapturer(rowInstance.getClass()).capture(join.getSetter());
					PropertyAccessor<Object, Object> accessor = PropertyAccessor.of(capture);
					// TODO: generic: la signature en Iterable du join.getSetter est fausse en OneToOne, je me demande comment ça marche ! il faudra sans doute mettre "Object" au lieu de Iterable
					// TODO: fournir également le getter (pour remplacer capture et supprimer spy du pom.xml) car c'est obligatoire pour avoir les "value bean" comme les Collection ou Embeddable
					// TODO: supprimer méthode deprecated JoinedStrategiesSelect#add
					// TODO: construire la Queue 1 fois et la réutiliser avec un Iterator ?
					// TODO: implémenter l'héritage d'entité
					// TODO: implémenter ManyToMany ?
					// TODO: supprimer la dépendance du projet vers spy (mis juste pour MethodReferenceCapturer)
					if (join.getOneToManyType() != null) {
						Collection<Object> oneToManyTarget = (Collection) accessor.get(rowInstance);
						if (oneToManyTarget == null) {
							oneToManyTarget = Reflections.newInstance((Class<Collection>) join.getOneToManyType());
						}
						oneToManyTarget.add(rowInstance2);
						join.getSetter().accept(rowInstance, oneToManyTarget);
					} else {
						join.getSetter().accept(rowInstance, rowInstance2);
					}
					stack.add(join.getStrategy());
				}
			}
		}
		return result;
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
