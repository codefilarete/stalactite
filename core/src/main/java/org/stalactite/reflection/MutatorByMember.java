package org.stalactite.reflection;

import java.lang.reflect.Member;

/**
 * @author Guillaume Mary
 */
public interface MutatorByMember<C, T, M extends Member> extends IMutator<C, T> {
	
	M getSetter();
}
