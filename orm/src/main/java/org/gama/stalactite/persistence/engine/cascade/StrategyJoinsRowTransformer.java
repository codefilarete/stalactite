package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins.MergeJoin;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins.RelationJoin;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Tranformer of a graph of joins to a graph of entities.
 * 
 * Non Thread-safe : contains a cache of bean being loaded.
 * 
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
	
	private final StrategyJoins<T> rootStrategyJoins;
	private Function<Column, String> aliasProvider = DEFAULT_ALIAS_PROVIDER;
	
	public StrategyJoinsRowTransformer(StrategyJoins<T> rootStrategyJoins) {
		this.rootStrategyJoins = rootStrategyJoins;
	}
	
	public StrategyJoinsRowTransformer(StrategyJoins<T> rootStrategyJoins, Function<Column, String> aliasProvider) {
		this.rootStrategyJoins = rootStrategyJoins;
		this.aliasProvider = aliasProvider;
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
	 * @param aliasProvider mapper between {@link Column} and their alias in the Rows of {@link #transform(Iterable, int, Map)}
	 */
	public void setAliasProvider(Function<Column, String> aliasProvider) {
		this.aliasProvider = aliasProvider;
	}
	
	/**
	 * 
	 * @param rows
	 * @param resultSize
	 * @param entityCache used for filling relations by getting beans them from it instead of creating clones. Will be filled by newly created entity.
	 * 					  Can be used to set a bigger cache coming from a wider scope. 
	 * @return
	 */
	public List<T> transform(Iterable<Row> rows, int resultSize, Map<Class, Map<Object /* identifier */, Object /* entity */>> entityCache) {
		List<T> result = new ArrayList<>(resultSize);
		EntityCacheWrapper entityCacheWrapper = new EntityCacheWrapper(entityCache);
		
		for (Row row : rows) {
			// Algorithm : we iterate depth by depth the tree structure of the joins
			// We start by the root of the hierarchy.
			// We process the entity of the current depth, then process the direct relations, add those relations to the depth iterator
			
			Nullable<T> newInstance = transform(entityCacheWrapper, row);
			newInstance.invoke(result::add);
		}
		return result;
	}
	
	public Nullable<T> transform(EntityCacheWrapper entityCacheWrapper, Row row) {
		Nullable<T> result = Nullable.empty();
		ColumnedRow columnedRow = new ColumnedRow(aliasProvider);
		Queue<StrategyJoins> stack = new ArrayDeque<>();
		stack.add(rootStrategyJoins);
		// we use a local cache of bean tranformer because we'll ask a slide of them with aliasProvider which creates an instance at each invokation
		Map<IEntityMappingStrategy, AbstractTransformer> beanTransformerCache = new HashMap<>();
		while (!stack.isEmpty()) {
			
			// treating the current depth
			StrategyJoins<Object> strategyJoins = stack.poll();
			IEntityMappingStrategy<Object, Object, Table> leftStrategy = strategyJoins.getStrategy();
			AbstractTransformer mainRowTransformer = beanTransformerCache.computeIfAbsent(leftStrategy, s -> s.copyTransformerWithAliases(columnedRow));
			Object identifier = leftStrategy.getIdMappingStrategy().getIdentifierAssembler().assemble(row, columnedRow);
			
			Object rowInstance = entityCacheWrapper.computeIfAbsent(leftStrategy.getClassToPersist(), identifier, () -> {
				Object newInstance = mainRowTransformer.transform(row);
				if (strategyJoins == rootStrategyJoins) {
					result.elseSet((T) newInstance);
				}
				return newInstance;
			});
			
			// processing the direct relations
			for (AbstractJoin join : strategyJoins.getJoins()) {
				StrategyJoins subJoins = join.getStrategy();
				IEntityMappingStrategy rightStrategy = subJoins.getStrategy();
				AbstractTransformer rowTransformer = beanTransformerCache.computeIfAbsent(rightStrategy, s -> s.copyTransformerWithAliases(columnedRow));
				if (join instanceof MergeJoin) {
					rowTransformer.applyRowToBean(row, rowInstance);
					// Adds the right strategy for further processing if it has some more joins so they'll also be taken into account
					if (!subJoins.getJoins().isEmpty()) {
						stack.add(subJoins);
					}
				} else if (join instanceof RelationJoin) {
					
					Object rightIdentifier = rightStrategy.getIdMappingStrategy().getIdentifierAssembler().assemble(row, columnedRow);
					
					// primary key null means no entity => nothing to do
					if (rightIdentifier != null) {
						Object rightInstance = entityCacheWrapper.computeIfAbsent(rightStrategy.getClassToPersist(), rightIdentifier,
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
		 * Main class that tries to retrieve an entity by its class and identifier or instanciates it and put it into the cache
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
}
