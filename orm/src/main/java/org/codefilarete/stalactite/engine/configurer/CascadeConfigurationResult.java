package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.engine.runtime.ConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * @author Guillaume Mary
 */
public class CascadeConfigurationResult<SRC, TRGT> {
	
	private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
	
	private final ConfiguredJoinedTablesPersister<SRC, ?> sourcePersister;
	
	public CascadeConfigurationResult(BeanRelationFixer<SRC, TRGT> beanRelationFixer,
									  ConfiguredJoinedTablesPersister<SRC, ?> sourcePersister) {
		this.beanRelationFixer = beanRelationFixer;
		this.sourcePersister = sourcePersister;
	}
	
	
	public BeanRelationFixer<SRC, TRGT> getBeanRelationFixer() {
		return beanRelationFixer;
	}
	
	public <SRCID> ConfiguredJoinedTablesPersister<SRC, SRCID> getSourcePersister() {
		return (ConfiguredJoinedTablesPersister<SRC, SRCID>) sourcePersister;
	}
	
}
