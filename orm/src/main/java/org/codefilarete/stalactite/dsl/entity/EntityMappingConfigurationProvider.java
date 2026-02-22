package org.codefilarete.stalactite.dsl.entity;

/**
 * @author Guillaume Mary
 */
public interface EntityMappingConfigurationProvider<C, I> {
	
	EntityMappingConfiguration<C, I> getConfiguration();
	
	/**
	 * A default {@link EntityMappingConfigurationProvider}
	 * Usage example:
	 * <pre>{@code
	 * EntityMappingConfigurationProviderHolder<Person, Long> personMappingConfiguration = new EntityMappingConfigurationProviderHolder<>();
	 * personMappingConfiguration.setProvider(
	 *     FluentMappings.entityBuilder(Person.class, Long.class)
	 *     ...
	 *     .mapOneToOne(Person::getHouse,
	 *         FluentMappings.entityBuilder(House.class, Long.class)
	 *         ...	
	 *         .mapOneToOne(House::getGardener, personMappingConfiguration)
	 *    )
	 * }</pre>
	 * 
	 * @param <C>
	 * @param <I>
	 * @author Guillaume Mary
	 */
	class EntityMappingConfigurationProviderHolder<C, I> implements EntityMappingConfigurationProvider<C, I> {
		
		private FluentEntityMappingBuilder<C, I> provider;
		
		public EntityMappingConfigurationProviderHolder() {
		}
		
		public void setProvider(FluentEntityMappingBuilder<C, I> provider) {
			this.provider = provider;
		}
		
		public FluentEntityMappingBuilder<C, I> getProvider() {
			return provider;
		}
		
		@Override
		public EntityMappingConfiguration<C, I> getConfiguration() {
			return provider.getConfiguration();
		}
	}
	
}
