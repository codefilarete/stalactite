package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.EmbeddableMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityLinkageByColumn;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * Parent class of entity mapping builders
 * 
 * @author Guillaume Mary
 * @param <C> entity type
 * @param <I> identifier type
 */
public abstract class AbstractEntityMappingBuilder<C, I> {
	
	/**
	 * Tracker of entities that are mapped along the build process. Used for column naming of relations : columns that target an entity may use a
	 * different strategy than simple properties, in particular for reverse column naming or bidirectional relation.
	 * Made static because several {@link EntityMappingBuilder}s are instanciated along the build process.
	 * Not the best design ever, but works !
	 */
	private static final ThreadLocal<Set<Class>> ENTITY_CANDIDATES = new ThreadLocal<>();
	
	/**
	 * Internal marker for this instance to cleanup {@link #ENTITY_CANDIDATES}. Made because multiple {@link EntityMappingBuilder} are recursively
	 * created to build an aggregate.
	 */
	private final boolean isInitiator;
	
	protected final EntityMappingConfiguration<C, I> configurationSupport;
	
	/** A helper to find method in classes, passed as reference because it contains a cache */
	protected final MethodReferenceCapturer methodSpy;
	
	protected final ColumnNameProvider columnNameProvider;
	
	public AbstractEntityMappingBuilder(EntityMappingConfiguration<C, I> entityMappingConfiguration, MethodReferenceCapturer methodSpy) {
		this.isInitiator = ENTITY_CANDIDATES.get() == null;
		
		if (isInitiator) {
			ENTITY_CANDIDATES.set(collectEntityCandidates(entityMappingConfiguration));
		}
		
		this.methodSpy = methodSpy;
		
		// Taking inheritance into account for column naming strategy : child classes take precedence over upper ones
		EmbeddableMappingConfiguration<C> effectiveConfiguration = giveEffectiveConfiguration(entityMappingConfiguration.getPropertiesMapping());
		// The easiest solution avoiding to re-create almost the same instances expect nmaing strategy, is to create a proxy for it which is done
		// through MethodReferenceDispatcher
		this.configurationSupport = new MethodReferenceDispatcher()
				.redirect(EntityMappingConfiguration<C, I>::getPropertiesMapping, () -> effectiveConfiguration)
				.fallbackOn(entityMappingConfiguration)
				.build(EntityMappingConfiguration.class);
		
		this.columnNameProvider = new ColumnNameProvider(effectiveConfiguration.getColumnNamingStrategy()) {
			/** Overriden to invoke join column naming strategy if necessary */
			@Override
			protected String giveColumnName(Linkage linkage) {
				if (ENTITY_CANDIDATES.get().contains(linkage.getColumnType())) {
					return configurationSupport.getJoinColumnNamingStrategy().giveName(MemberDefinition.giveMemberDefinition(linkage.getAccessor()));
				} else {
					return super.giveColumnName(linkage);
				}
			}
		};
	}
	
	private Set<Class> collectEntityCandidates(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		Set<Class> result = new HashSet<>();
		
		for (EntityMappingConfiguration mappingConfiguration : entityMappingConfiguration.inheritanceIterable()) {
			result.add(mappingConfiguration.getEntityType());
			result.addAll(collectRelationEntities(mappingConfiguration));
		}
		
		return result;
		
	}
	
	private Set<Class> collectRelationEntities(EntityMappingConfiguration<?, ?> entityMappingConfiguration) {
		Set<Class> result = new HashSet<>();
		// one to manys
		Iterables.collect(entityMappingConfiguration.getOneToManys(), cascadeMany -> cascadeMany.getTargetMappingConfiguration().getEntityType(), () -> result);
		// one to ones
		Iterables.collect(entityMappingConfiguration.getOneToOnes(), cascadeOne -> cascadeOne.getTargetMappingConfiguration().getEntityType(), () -> result);
		// we add ourselve for any bidirectional relation (could have been done only in case of real bidirectionality)
		result.add(entityMappingConfiguration.getEntityType());
		
		return result;
	}
	
	/**
	 * Build a {@link Persister} from the given configuration at contruction time.
	 * {@link Table}s and columns will be created to fulfill the requirements needed by the configuration.
	 *
	 * @param persistenceContext the {@link PersistenceContext} in which the resulting {@link Persister} will be put, also needed for Dialect purpose 
	 * @return the built {@link Persister}
	 */
	public JoinedTablesPersister<C, I, Table> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, null);
	}
	
	public <T extends Table> JoinedTablesPersister<C, I, T> build(PersistenceContext persistenceContext, @Nullable T targetTable) {
		try {
			// Very first thing, determine identifier management and check some configuration
			if (configurationSupport.getIdentifierAccessor() != null && configurationSupport.getInheritanceConfiguration() != null
					// TODO : check for removal of this condition when subclasses will be defined with EmbeddableConfiguration, not EntityConfiguration
					&& configurationSupport.getInheritanceConfiguration().getPolymorphismPolicy() == null) {
				throw new MappingConfigurationException("Defining an identifier while inheritance is used is not supported : "
						+ Reflections.toString(configurationSupport.getEntityType()) + " defined identifier " + MemberDefinition.toString(configurationSupport.getIdentifierAccessor())
						+ " while it inherits from " + Reflections.toString(configurationSupport.getInheritanceConfiguration().getEntityType()));
			}
			
			// Table must be created before giving it to further methods because it is mandatory for them
			if (targetTable == null) {
				targetTable = (T) nullable(giveTableUsedInMapping()).getOr(() -> new Table(configurationSupport.getTableNamingStrategy().giveName(configurationSupport.getEntityType())));
			}
			return this.doBuild(persistenceContext, targetTable);
		} finally {
			// we clear entity candidates in any cases
			if (this.isInitiator) {
				ENTITY_CANDIDATES.remove();
			}
		}
	}
	
	protected abstract <T extends Table<?>> JoinedTablesPersister<C, I, T> doBuild(PersistenceContext persistenceContext, T targetTable);
	
	@Nullable
	protected Table giveTableUsedInMapping() {
		Set<Table> usedTablesInMapping = Iterables.collect(configurationSupport.getPropertiesMapping().getPropertiesMapping(),
				linkage -> linkage instanceof FluentEntityMappingConfigurationSupport.EntityLinkageByColumn,
				linkage -> ((EntityLinkageByColumn) linkage).getColumn().getTable(),
				HashSet::new);
		switch (usedTablesInMapping.size()) {
			case 0:
				return null;
			case 1:
				return Iterables.first(usedTablesInMapping);
			default:
				throw new MappingConfigurationException("Different tables found in columns given as parameter of methods mapping : " + usedTablesInMapping);
		}
	}
	
	/**
	 * Creates a proxy that gives the effective configuration (about column naming strategy) by taking inheritance into account
	 * 
	 * @param embeddableMappingConfiguration the original configuration
	 * @param <E> original entity type
	 * @return a proxy that wraps the original configuration except for column naming strategy (which the "lowest" class is returned)
	 */
	protected static <E> EmbeddableMappingConfiguration<E> giveEffectiveConfiguration(EmbeddableMappingConfiguration<E> embeddableMappingConfiguration) {
		EmbeddableMappingConfiguration<E> result = embeddableMappingConfiguration;
		if (embeddableMappingConfiguration.getMappedSuperClassConfiguration() != null) {
			// When a ColumnNamingStrategy is defined on mapping, it must be applied to super classes too, it must pass their configuration.
			// But EmbeddableMappingConfiguration is not writable (only getters) so we create a proxy around super class configuration that returns
			// current class ColumnNamingStrategy 
			if (embeddableMappingConfiguration.getColumnNamingStrategy() != null) {
				// NB: we can't mutualize MethodReferenceDispatcher instances because they handle same classes with different dispatch,
				// so having only one instance for them result in a stackoverflow error
				EmbeddableMappingConfiguration mappedSuperClassConfiguration = new MethodReferenceDispatcher()
						.redirect(EmbeddableMappingConfiguration<ColumnNamingStrategy>::getColumnNamingStrategy, embeddableMappingConfiguration::getColumnNamingStrategy)
						.fallbackOn(embeddableMappingConfiguration.getMappedSuperClassConfiguration())
						.build(EmbeddableMappingConfiguration.class);
				result = new MethodReferenceDispatcher()
						.redirect(EmbeddableMappingConfiguration<ColumnNamingStrategy>::getMappedSuperClassConfiguration, () -> mappedSuperClassConfiguration)
						.fallbackOn(embeddableMappingConfiguration)
						.build(EmbeddableMappingConfiguration.class);
			} else {
				result = new MethodReferenceDispatcher()
						.redirect(EmbeddableMappingConfiguration<ColumnNamingStrategy>::getColumnNamingStrategy, embeddableMappingConfiguration.getMappedSuperClassConfiguration()::getColumnNamingStrategy)
						.fallbackOn(embeddableMappingConfiguration)
						.build(EmbeddableMappingConfiguration.class);
			}
		}
		return result;
	}
	
}
