package org.stalactite.reflection;

import java.lang.reflect.Member;

/**
 * @author Guillaume Mary
 */
public interface AccessorByMember<C, T, M extends Member> extends IAccessor<C, T>{
	
	M getGetter();
}
