package org.stalactite.reflection;

import java.lang.reflect.Member;

/**
 * @author Guillaume Mary
 */
public interface MutatorByMember<M extends Member> {
	
	M getSetter();
}
