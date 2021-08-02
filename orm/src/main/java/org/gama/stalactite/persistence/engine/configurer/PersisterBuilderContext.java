package org.gama.stalactite.persistence.engine.configurer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityPersister;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Running context of {@link PersisterBuilderImpl}. Allows to share some information during configuraiton of the whole entity graph.
 * The currently available instance is accessible through {@link #CURRENT} variable which is created and destroyed by
 * {@link PersisterBuilderImpl#build(Dialect, ConnectionConfiguration, PersisterRegistry, Table)} 
 * 
 * @author Guillaume Mary
 */
public class PersisterBuilderContext {
	
	/**
	 * Give access to current {@link PersisterBuilderContext}, created and destroyed by
	 * {@link PersisterBuilderImpl#build(Dialect, ConnectionConfiguration, PersisterRegistry, Table)}
	 * 
	 * Made static because several {@link PersisterBuilderImpl}s are instanciated along the build process.
	 * Not the best design ever, but works !
	 */
	static final ThreadLocal<PersisterBuilderContext> CURRENT = new ThreadLocal<>();
	
	/**
	 * Tracker of entities that are mapped along the build process. Used for column naming of relations : columns that target an entity may use a
	 * different strategy than simple properties, in particular for reverse column naming or bidirectional relation.
	 */
	private final Set<Class> entityCandidates = new HashSet<>();
	
	/**
	 * List of post initializers to be invoked after persister instanciation and main configuration
	 */
	private final List<PostInitializer<Object>> postInitializers = new ArrayList<>();
	
	/**
	 * Currently treated configurations. Made to detect cycle in graph. 
	 */
	private final Queue<Class> treatedConfigurations = Collections.asLifoQueue(new ArrayDeque<>());
	
	void addEntity(Class<?> entityType) {
		this.entityCandidates.add(entityType);
	}
	
	boolean isEntity(Class<?> entityType) {
		return entityCandidates.contains(entityType);
	}
	
	public List<PostInitializer<Object>> getPostInitializers() {
		return postInitializers;
	}
	
	public void addPostInitializers(PostInitializer<?> postInitializer) {
		postInitializers.add((PostInitializer<Object>) postInitializer);
	}
	
	/**
	 * Executes some code by remembering given configuration as treated so cycle (already configured entity) can be detected through {@link #isCycling(EntityMappingConfiguration)}
	 * 
	 * @param entityMappingConfiguration a configuration to remember as being currently treated
	 * @param callable some code to run
	 */
	public void runInContext(EntityMappingConfiguration<?, ?> entityMappingConfiguration, Runnable callable) {
		runInContext(entityMappingConfiguration.getEntityType(), callable);
	}
	
	/**
	 * Executes some code by remembering given configuration as treated so cycle (already configured entity) can be detected through {@link #isCycling(EntityMappingConfiguration)}
	 *
	 * @param entityMappingConfiguration a configuration to remember as being currently treated
	 * @param callable some code to run
	 */
	public void runInContext(EntityPersister<?, ?> entityMappingConfiguration, Runnable callable) {
		runInContext(entityMappingConfiguration.getClassToPersist(), callable);
	}
	
	private void runInContext(Class entityClass, Runnable callable) {
		try {
			treatedConfigurations.add(entityClass);
			callable.run();
		} finally {
			treatedConfigurations.remove();
		}
	}
	
	/**
	 * Checks if given configuration was already treated by current build process. {@link #runInContext(EntityMappingConfiguration, Runnable)} should
	 * have be invoked somehow before.
	 *
	 * @param entityMappingConfiguration configuration to be checked for cycle
	 * @return true if given configuration was already processed
	 */
	boolean isCycling(EntityMappingConfiguration<?, ?> entityMappingConfiguration) {
		return treatedConfigurations.contains(entityMappingConfiguration.getEntityType());
	}
}
