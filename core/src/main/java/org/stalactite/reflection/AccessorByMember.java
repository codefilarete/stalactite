package org.stalactite.reflection;

import java.lang.reflect.Member;

/**
 * @author Guillaume Mary
 */
public interface AccessorByMember<M extends Member> {
	
	M getGetter();
}
