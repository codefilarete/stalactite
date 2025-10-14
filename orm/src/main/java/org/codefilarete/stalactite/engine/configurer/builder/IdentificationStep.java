package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.CompositeKeyMapping;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.KeyMapping;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.SingleKeyMapping;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.function.Hanger.Holder;

/**
 * Determine identification (single/composite), perform mapping-superclass + inheritance constraints, ensure equals/hashCode constraints for composite.
 *
 * @author Guillaume Mary
 */
public class IdentificationStep<C, I> extends AbstractIdentificationStep<C, I> {
	
	/**
	 * Looks for {@link EntityMappingConfiguration} defining the identifier policy by going up through inheritance hierarchy.
	 *
	 * @return a wrapper of necessary elements to manage entity identifier
	 * @throws UnsupportedOperationException when identification was not found, because it doesn't make sense to have an entity without identification
	 */
	AbstractIdentification<C, I> determineIdentification(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		if (entityMappingConfiguration.getInheritanceConfiguration() != null && entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			throw new MappingConfigurationException("Combination of mapped super class and inheritance is not supported, please remove one of them");
		}
		if (entityMappingConfiguration.getKeyMapping() != null && entityMappingConfiguration.getInheritanceConfiguration() != null) {
			throw new MappingConfigurationException("Defining an identifier in conjunction with entity inheritance is not supported : "
					+ Reflections.toString(entityMappingConfiguration.getEntityType()) + " defines identifier "
					+ AccessorDefinition.toString(entityMappingConfiguration.getKeyMapping().getAccessor())
					+ " while it inherits from " + Reflections.toString(entityMappingConfiguration.getInheritanceConfiguration().getConfiguration().getEntityType()));
		}
		
		// if mappedSuperClass is used, then identifier is expected to be declared on the configuration
		// because mappedSuperClass can't define it (it is an EmbeddableMappingConfiguration)
		Holder<EntityMappingConfiguration<C, I>> configurationDefiningIdentification = new Holder<>();
		// hierarchy must be scanned to find the very first configuration that defines identification
		// iterating over mapping from inheritance
		entityMappingConfiguration.inheritanceIterable().forEach(entityConfiguration -> {
			KeyMapping keyMapping = entityConfiguration.getKeyMapping();
			if (keyMapping != null &&
					(keyMapping instanceof EntityMappingConfiguration.SingleKeyMapping && ((SingleKeyMapping<?, ?>) keyMapping).getIdentifierPolicy() != null)
					|| keyMapping instanceof EntityMappingConfiguration.CompositeKeyMapping) {
				if (configurationDefiningIdentification.get() != null) {
					throw new UnsupportedOperationException("Identifier policy is defined twice in hierarchy : first by "
							+ Reflections.toString(configurationDefiningIdentification.get().getEntityType())
							+ ", then by " + Reflections.toString(entityConfiguration.getEntityType()));
				} else {
					configurationDefiningIdentification.set((EntityMappingConfiguration<C, I>) entityConfiguration);
				}
			}
		});
		EntityMappingConfiguration<C, I> foundConfiguration = configurationDefiningIdentification.get();
		if (foundConfiguration == null) {
			throw newMissingIdentificationException(entityMappingConfiguration.getEntityType());
		}
		
		KeyMapping<C, I> foundKeyMapping = foundConfiguration.getKeyMapping();
		if (foundKeyMapping instanceof SingleKeyMapping) {
			return AbstractIdentification.forSingleKey(foundConfiguration);
		} else if (foundKeyMapping instanceof CompositeKeyMapping) {
			assertCompositeKeyIdentifierOverridesEqualsHashcode((CompositeKeyMapping<?, ?>) foundKeyMapping);
			return AbstractIdentification.forCompositeKey(foundConfiguration, (CompositeKeyMapping<C, I>) foundKeyMapping);
		} else {
			// should not happen
			throw new MappingConfigurationException("Unknown key mapping : " + foundKeyMapping);
		}
	}
	
	@VisibleForTesting
	static void assertCompositeKeyIdentifierOverridesEqualsHashcode(CompositeKeyMapping<?, ?> compositeKeyIdentification) {
		Class<?> compositeKeyType = AccessorDefinition.giveDefinition(compositeKeyIdentification.getAccessor()).getMemberType();
		try {
			compositeKeyType.getDeclaredMethod("equals", Object.class);
			compositeKeyType.getDeclaredMethod("hashCode");
		} catch (NoSuchMethodException e) {
			throw new MappingConfigurationException("Composite key identifier class " + Reflections.toString(compositeKeyType) + " seems to have default implementation of equals() and hashcode() methods,"
					+ " which is not supported (identifiers must be distinguishable), please make it implement them");
		}
	}
}
