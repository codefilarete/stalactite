package org.gama.stalactite.persistence.mapping;

import java.util.Set;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Mapper for embedded beans in another. For instance a simple @OneToOne can be considered as an embedded bean.
 * 
 * @author Guillaume Mary
 */
public interface IEmbeddedBeanMapper<C, T extends Table> extends IMappingStrategy<C, T> {
	
	Set<Column<T, Object>> getColumns();
}
