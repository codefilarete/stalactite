package org.codefilarete.stalactite.engine.configurer.property;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.sql.ddl.Size;

public interface LocalColumnLinkageOptions extends EntityMappingConfiguration.ColumnLinkageOptions {
	
	void setColumnName(String columnName);
	
	void setColumnSize(Size columnSize);
}
