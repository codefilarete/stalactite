package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Parent class for one-to-many configurer: subclasses handle relation mapped by attribute and association table.
 * 
 * @param <SRC> type of source entity (one side)
 * @param <TRGT> type of target entity (many side)
 * @param <SRCID> type of source entity identifier
 * @param <TRGTID> type of target entity identifier
 * @param <C> collection type that stores the relation
 * @param <LEFTTABLE> table type of source entity 
 * @author Guillaume Mary
 */
abstract class OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>> {
	
	protected final OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> associationConfiguration;
	protected final boolean loadSeparately;
	protected final AccessorDefinition accessorDefinitionForTableNaming;
	
	protected OneToManyConfigurerTemplate(OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, LEFTTABLE> associationConfiguration,
										  boolean loadSeparately) {
		this.associationConfiguration = associationConfiguration;
		this.loadSeparately = loadSeparately;
		AccessorDefinition accessorDefinition = associationConfiguration.getAccessorDefinition();
		this.accessorDefinitionForTableNaming = new AccessorDefinition(
				accessorDefinition.getDeclaringClass(),
				accessorDefinition.getName(),
				// we prefer the target persister type to method reference member type because the latter only gets the collection type which is not
				// valuable information for table / column naming
				associationConfiguration.getOneToManyRelation().getTargetMappingConfiguration().getEntityType());
	}
	
	protected abstract String configure(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister);
	
	public abstract CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																					   ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																					   FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener);
}
