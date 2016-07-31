package org.gama.stalactite.persistence.mapping;

import java.util.Set;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Mapper for embedded beans in another. For instance a simple @OneToOne can be considered as an embedded bean.
 * 
 * @author Guillaume Mary
 */
public interface IEmbeddedBeanMapper<T> extends IMappingStrategy<T> {
	
	Set<Column> getColumns();
}
