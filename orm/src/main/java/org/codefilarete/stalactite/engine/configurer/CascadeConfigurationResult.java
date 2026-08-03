package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalEntityPersister;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * @author Guillaume Mary
 */
public class CascadeConfigurationResult<SRC, TRGT> {
	
	private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
	
	private final EntityWriteExecutor<SRC, ?> sourcePersister;
	
	public CascadeConfigurationResult(BeanRelationFixer<SRC, TRGT> beanRelationFixer,
	                                  EntityWriteExecutor<SRC, ?> sourcePersister) {
		this.beanRelationFixer = beanRelationFixer;
		this.sourcePersister = sourcePersister;
	}
	
	
	public BeanRelationFixer<SRC, TRGT> getBeanRelationFixer() {
		return beanRelationFixer;
	}
	
	public <SRCID> ConfiguredRelationalEntityPersister<SRC, SRCID, ?> getSourcePersister() {
		return (ConfiguredRelationalEntityPersister<SRC, SRCID, ?>) sourcePersister;
	}
	
}
