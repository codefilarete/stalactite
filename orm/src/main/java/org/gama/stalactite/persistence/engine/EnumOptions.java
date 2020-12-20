package org.gama.stalactite.persistence.engine;

/**
 * Enum mapping options
 * 
 * @author Guillaume Mary
 */
public interface EnumOptions extends PropertyOptions {
	
	EnumOptions byName();
	
	EnumOptions byOrdinal();
	
	@Override
	EnumOptions mandatory();
}
