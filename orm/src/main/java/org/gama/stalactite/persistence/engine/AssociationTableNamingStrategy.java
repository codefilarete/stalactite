package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.structure.Column;

/**
 * Contract to give a name to an association table (one-to-many cases)
 * 
 * @author Guillaume Mary
 */
public interface AssociationTableNamingStrategy {
	
	String giveName(Column src, Column target);
	
	String giveOneSideColumnName(Column src);
	
	default String giveManySideColumnName(Column src) {
		return giveOneSideColumnName(src);
	}
	
	AssociationTableNamingStrategy DEFAULT = new AssociationTableNamingStrategy() {
		@Override
		public String giveName(Column src, Column target) {
			return src.getTable().getName() + "_" + target.getTable().getName() + "s";
		}
		
		@Override
		public String giveOneSideColumnName(Column src) {
			return src.getTable().getName() + "_" + src.getName();
		}
	};
}
