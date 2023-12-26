package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * @author Guillaume Mary
 */
public class CascadeConfigurationResult<SRC, TRGT> {
	
	private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
	
	private final ConfiguredRelationalPersister<SRC, ?> sourcePersister;
	
	public CascadeConfigurationResult(BeanRelationFixer<SRC, TRGT> beanRelationFixer,
									  ConfiguredRelationalPersister<SRC, ?> sourcePersister) {
		this.beanRelationFixer = beanRelationFixer;
		this.sourcePersister = sourcePersister;
	}
	
	
	public BeanRelationFixer<SRC, TRGT> getBeanRelationFixer() {
		return beanRelationFixer;
	}
	
	public <SRCID> ConfiguredRelationalPersister<SRC, SRCID> getSourcePersister() {
		return (ConfiguredRelationalPersister<SRC, SRCID>) sourcePersister;
	}
	
}
