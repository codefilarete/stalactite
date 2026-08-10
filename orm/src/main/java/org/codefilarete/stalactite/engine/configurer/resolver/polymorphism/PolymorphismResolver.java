package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism;

import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.resolver.CreatedPersisterCollector;
import org.codefilarete.stalactite.engine.runtime.PolymorphicWriter;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public interface PolymorphismResolver<P extends PolymorphicWriter> {
	
	<TRGT, TRGTID, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT, DTYPE>
	P resolve(PolymorphicEntity<TRGT, TRGTID, RIGHTTABLE> targetEntity,
	          CreatedPersisterCollector<TRGT, TRGTID> persisterCollector);
}
