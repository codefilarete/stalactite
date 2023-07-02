package org.codefilarete.stalactite.engine.configurer;

import java.util.Map;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Stores information about entity identification during configuration process.
 * Two cases are expected :
 * - identification for single-column primary key, stored in {@link Identification}
 * - identification for multiple-columns primary key, stored in {@link CompositeKeyIdentification}
 * 
 * All this is not expected to be exposed out of configuration process.
 *
 * @param <C> entity type
 * @param <I> identifier type
 * @author Guillaume Mary
 * @see #forSingleKey(EntityMappingConfiguration)
 * @see #forCompositeKey(EntityMappingConfiguration)
 */
public abstract class AbstractIdentification<C, I> {
	
	/**
	 * Build a single-key identification definition.
	 * Argument identificationDefiner is expected to have a {@link org.codefilarete.stalactite.engine.EntityMappingConfiguration.SingleKeyMapping}
	 * as key mapping, else a {@link ClassCastException} will be thrown.
	 * 
	 * @param identificationDefiner a configuration that defines a single-key identifier.
	 * @param <C> identified entity
	 * @param <I> identifier type
	 * @return a single-key identification
	 */
	static <C, I> Identification<C, I> forSingleKey(EntityMappingConfiguration<C, I> identificationDefiner) {
		return new Identification<>(identificationDefiner, ((EntityMappingConfiguration.SingleKeyMapping<C, I>) identificationDefiner.getKeyMapping()).getIdentifierPolicy());
	}
	
	/**
	 * Build a composite-key identification definition.
	 * Composite-key identification is always an already-defined identifier policy.
	 * 
	 * @param identificationDefiner a configuration that defines a composite-key identifier.
	 * @param <C> identified entity
	 * @param <I> identifier type
	 * @return a composite-key identification
	 */
	static <C, I> CompositeKeyIdentification<C, I> forCompositeKey(EntityMappingConfiguration<C, I> identificationDefiner) {
		return new CompositeKeyIdentification<>(identificationDefiner);
	}
	
	private final ReversibleAccessor<C, I> idAccessor;
	private final EntityMappingConfiguration<C, I> identificationDefiner;
	private final EntityMappingConfiguration.KeyMapping<C, I> keyLinkage;
	
	/**
	 * Insertion manager for {@link ClassMapping} that owns identifier policy
	 */
	private IdentifierInsertionManager<C, I> insertionManager;
	
	/**
	 * Insertion manager for {@link ClassMapping} that doesn't own identifier policy : they get an already-assigned one.
	 * Set for entity that inherits another one which defines identifier policy.
	 */
	private AlreadyAssignedIdentifierManager<C, I> fallbackInsertionManager;
	
	private AbstractIdentification(EntityMappingConfiguration<C, I> identificationDefiner) {
		this.keyLinkage = identificationDefiner.getKeyMapping();
		this.idAccessor = identificationDefiner.getKeyMapping().getAccessor();
		this.identificationDefiner = identificationDefiner;
	}
	
	public EntityMappingConfiguration.KeyMapping<C, I> getKeyLinkage() {
		return keyLinkage;
	}
	
	public ReversibleAccessor<C, I> getIdAccessor() {
		return idAccessor;
	}
	
	public EntityMappingConfiguration<C, I> getIdentificationDefiner() {
		return identificationDefiner;
	}
	
	public IdentifierInsertionManager<C, I> getInsertionManager() {
		return insertionManager;
	}
	
	public AbstractIdentification<C, I> setInsertionManager(IdentifierInsertionManager<C, I> insertionManager) {
		this.insertionManager = insertionManager;
		return this;
	}
	
	public AlreadyAssignedIdentifierManager<C, I> getFallbackInsertionManager() {
		return fallbackInsertionManager;
	}
	
	public AbstractIdentification<C, I> setFallbackInsertionManager(AlreadyAssignedIdentifierManager<C, I> fallbackInsertionManager) {
		this.fallbackInsertionManager = fallbackInsertionManager;
		return this;
	}
	
	/**
	 * Identification for single-column case
	 *
	 * @param <C> persisted entity type
	 * @param <I> identifier type
	 */
	public static class Identification<C, I> extends AbstractIdentification<C, I> {
		
		private final ColumnOptions.IdentifierPolicy<I> identifierPolicy;
		
		private Identification(EntityMappingConfiguration<C, I> identificationDefiner, ColumnOptions.IdentifierPolicy<I> identifierPolicy) {
			super(identificationDefiner);
			this.identifierPolicy = identifierPolicy;
		}
		
		public ColumnOptions.IdentifierPolicy<I> getIdentifierPolicy() {
			return identifierPolicy;
		}
	}
	
	/**
	 * Identification for multiple-column case
	 *
	 * @param <C> persisted entity type
	 * @param <I> identifier type
	 */
	public static class CompositeKeyIdentification<C, I> extends AbstractIdentification<C, I> {
		
		private Map<ReversibleAccessor<I, Object>, Column<Table, Object>> compositeKeyMapping;
		
		private CompositeKeyIdentification(EntityMappingConfiguration<C, I> identificationDefiner) {
			super(identificationDefiner);
		}
		
		public Map<ReversibleAccessor<I, Object>, Column<Table, Object>> getCompositeKeyMapping() {
			return compositeKeyMapping;
		}
		
		public void setCompositeKeyMapping(Map<ReversibleAccessor<I, Object>, Column<Table, Object>> compositeKeyMapping) {
			this.compositeKeyMapping = compositeKeyMapping;
		}
	}
}
