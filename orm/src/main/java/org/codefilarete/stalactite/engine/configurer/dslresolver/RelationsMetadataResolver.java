package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.ArrayDeque;
import java.util.Deque;

import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.collection.KeepOrderSet;

public class RelationsMetadataResolver {
	
	private final OneToOneMetadataResolver oneToOneMetadataResolver;
	private final OneToManyMetadataResolver oneToManyMetadataResolver;
	private final ElementCollectionMetadataResolver elementCollectionMetadataResolver;
	
	public RelationsMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.oneToOneMetadataResolver = new OneToOneMetadataResolver(dialect, connectionConfiguration);
		this.oneToManyMetadataResolver = new OneToManyMetadataResolver(dialect, connectionConfiguration);
		this.elementCollectionMetadataResolver = new ElementCollectionMetadataResolver(dialect);
	}
	
	<C, I> void resolve(EntitySource<C, I> rootEntitySource) {
		
		// Non-recursive, stack-based depth-first traversal over the EntitySource tree.
		// Each element on the stack is an EntitySource to process, processing it produces
		// new EntitySources (the relation targets) which are pushed onto the stack.
		Deque<EntitySource<?, ?>> stack = new ArrayDeque<>();
		stack.push(rootEntitySource);
		
		while (!stack.isEmpty()) {
			EntitySource<?, ?> current = stack.pop();
			KeepOrderSet<EntitySource<?, ?>> childSources = resolveRelations(current);
			// Push children so they are processed in subsequent iterations (depth-first)
			for (EntitySource<?, ?> child : childSources) {
				stack.push(child);
			}
		}
	}
	
	/**
	 * Resolves all one-to-one and one-to-many relations for the given {@link EntitySource},
	 * covering both the entity's own configurations and its ancestors'.
	 *
	 * @return the set of {@link EntitySource}s produced by the resolved relations, to be enqueued for further traversal
	 */
	private <C, I, X> KeepOrderSet<EntitySource<?, ?>> resolveRelations(EntitySource<C, I> source) {
		
		KeepOrderSet<EntitySource<?, ?>> childSources = new KeepOrderSet<>();
		
		// --- Relations owned by this entity ---
		childSources.addAll(resolveForConfiguration(source));
		
		// --- Relations owned by ancestor entities ---
		source.<X>getAncestorSources()
				.forEach(entitySource ->
						childSources.addAll(resolveForConfiguration(entitySource)));
		
		return childSources;
	}
	
	/**
	 * Resolves all one-to-one and one-to-many relations (including those embedded in insets) and returns the resulting
	 * children.
	 */
	private <C, I> KeepOrderSet<EntitySource<?, ?>> resolveForConfiguration(EntitySource<C, I> source) {
		
		KeepOrderSet<EntitySource<?, ?>> newSourcesFound = new KeepOrderSet<>();
		
		// One-to-one relations directly on the entity and embedded in insets
		newSourcesFound.addAll(oneToOneMetadataResolver.resolve(source));
		// One-to-many relations directly on the entity and embedded in insets
		newSourcesFound.addAll(oneToManyMetadataResolver.resolve(source));
		// Collection of elements' relations directly on the entity and embedded in insets
		elementCollectionMetadataResolver.resolve(source);
		
		return newSourcesFound;
	}
}
