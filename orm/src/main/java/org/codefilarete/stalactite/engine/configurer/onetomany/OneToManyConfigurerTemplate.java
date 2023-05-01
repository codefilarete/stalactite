package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Parent class for one-to-many configurer : subclasses handle relation mapped by attribute and association table.
 * 
 * @param <SRC> type of source entity (one side)
 * @param <TRGT> type of target entity (many side)
 * @param <SRCID> type of source entity identifier
 * @param <TRGTID> type of target entity identifier
 * @param <C> collection type that stores the relation
 * @param <LEFTTABLE> table type of source entity 
 * @author Guillaume Mary
 */
public abstract class OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>> {
	
	protected final OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> associationConfiguration;
	protected final ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister;
	protected final boolean loadSeparately;
	
	/**
	 * Equivalent to {@link org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation#getMethodReference()} but used for table and colum naming only.
	 * Collection access will be done through {@link OneToManyAssociationConfiguration#getCollectionGetter()} and {@link OneToManyAssociationConfiguration#giveCollectionFactory()}
	 */
	protected AccessorDefinition accessorDefinition;
	
	protected OneToManyConfigurerTemplate(OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> associationConfiguration,
										  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
										  boolean loadSeparately) {
		this.associationConfiguration = associationConfiguration;
		this.targetPersister = targetPersister;
		this.loadSeparately = loadSeparately;
		determineAccessorDefinition();
	}
	
	/**
	 * Fixes this.accessorDefinition
	 */
	private void determineAccessorDefinition() {
		// we don't use AccessorDefinition.giveMemberDefinition(..) because it gives a cross-member definition, loosing get/set for example,
		// whereas we need this information to build better association table name
		this.accessorDefinition = new AccessorDefinition(
				associationConfiguration.getOneToManyRelation().getMethodReference().getDeclaringClass(),
				AccessorDefinition.giveDefinition(associationConfiguration.getOneToManyRelation().getMethodReference()).getName(),
				// we prefer target persister type to method reference member type because the latter only gets collection type which is not
				// a valuable information for table / column naming
				targetPersister.getClassToPersist());
	}
	
	protected abstract void configure();
	
	public abstract CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																					   FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener);
}
