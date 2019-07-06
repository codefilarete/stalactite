package org.gama.stalactite.persistence.engine;

/**
 * Declares a simple entry point to start configuring a persistence mapping.
 * 
 * @author Guillaume Mary
 */
public final class MappingEase {
	
	/**
	 * Will start a {@link IFluentEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted by the {@link org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister}
	 * 						 that will be created by {@link IFluentEntityMappingBuilder#build(PersistenceContext)}
	 * @param identifierType entity identifier type
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link IFluentEntityMappingBuilder}
	 */
	@SuppressWarnings("squid:S1172")	// identifierType is used to sign result
	public static <T, I> IFluentEntityMappingBuilder<T, I> mappingBuilder(Class<T> classToPersist, Class<I> identifierType) {
		return new FluentEntityMappingConfigurationSupport<>(classToPersist);
	}
	
	private MappingEase() {
		// tool class
	}
}
