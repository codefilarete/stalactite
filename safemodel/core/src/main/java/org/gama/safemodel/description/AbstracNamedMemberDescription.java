package org.gama.safemodel.description;

import java.lang.reflect.Member;

/**
 * 
 * @param <M> expected to be {@link java.lang.reflect.Field} or {@link java.lang.reflect.Method},
 *           using {@link java.lang.reflect.Constructor} should technically work but API doesn't expect it  
 * @author Guillaume Mary
 */
public abstract class AbstracNamedMemberDescription<M extends Member> extends AbstractMemberDescription {
	
	public final String name;
	
	public AbstracNamedMemberDescription(Class declaringClass, String name) {
		super(declaringClass);
		this.name = name;
	}
	
	protected AbstracNamedMemberDescription(M member) {
		this(member.getDeclaringClass(), member.getName());
		
	}
	
	public String getName() {
		return name;
	}
}
