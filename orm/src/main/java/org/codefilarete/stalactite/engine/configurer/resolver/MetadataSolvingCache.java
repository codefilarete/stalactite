package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

public class MetadataSolvingCache {
	
	private final Map<Entity<?, ?, ?>, EntitySource<?, ?>> entityBuildSources = new HashMap<>();
	
	public <X, I> void put(Entity<X, I, ?> entity, EntitySource<X, I> entitySource) {
		entityBuildSources.put(entity, entitySource);
	}
	
	public boolean containsEntityType(Class<?> entityType) {
		return entityBuildSources.containsKey(entityType);
	}
	
	/**
	 * Since there's no exact matching between entity and {@link ResolvedConfiguration}, in particular when
	 * dealing with inheritance, we need to keep track of ancestor configurations to resolve the final configuration.
	 * 
	 * @param <C>
	 * @param <I>
	 * @author Guillaume Mary
	 */
	public static class EntitySource<C, I> {
		
		private final Entity<C, I, ?> entity;
		
		private final Set<ResolvedConfiguration<C, I>> resolvedConfigurations = new KeepOrderSet<>();
		
		private final Map<Entity<?, ?, ?>, Set<ResolvedConfiguration<?, I>>> ancestorSources = new KeepOrderMap<>();
		
		EntitySource(Entity<C, I, ?> entity, ResolvedConfiguration<C, I> resolvedConfiguration) {
			this.entity = entity;
			this.resolvedConfigurations.add(resolvedConfiguration);
		}
		
		public <T extends Table<T>> Entity<C, I, T> getEntity() {
			return (Entity<C, I, T>) entity;
		}
		
		public Set<ResolvedConfiguration<C, I>> getResolvedConfigurations() {
			return resolvedConfigurations;
		}
		
		public void addSource(Entity<?, ?, ?> entityOrAncestor, ResolvedConfiguration<?, I> ancestorSource) {
			if (entityOrAncestor == entity){
				resolvedConfigurations.add((ResolvedConfiguration<C, I>) ancestorSource);
			} else {
				ancestorSources.computeIfAbsent(entityOrAncestor, k -> new KeepOrderSet<>()).add(ancestorSource);
			}
		}
		
		public <X> Map<Entity<X, I, ?>, Set<ResolvedConfiguration<X, I>>> getAncestorSources() {
			return (Map<Entity<X, I, ?>, Set<ResolvedConfiguration<X, I>>>) (Map) ancestorSources;
		}
		
		public Set<ResolvedConfiguration<?, I>> getAncestorSources(Entity<?, ?, ?> ancestor) {
			return ancestorSources.get(ancestor);
		}
	}
}
