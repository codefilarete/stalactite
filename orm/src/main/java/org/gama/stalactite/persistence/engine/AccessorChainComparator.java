package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.Reflections.MemberNotFoundException;
import org.gama.lang.StringAppender;
import org.gama.lang.bean.ClassIterator;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.AbstractReflector;
import org.gama.reflection.AccessorByMember;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.IAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferences;
import org.gama.reflection.MutatorByMember;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public class AccessorChainComparator {
	
	private static final MethodReferenceCapturer METHOD_REFERENCE_CAPTURER = new MethodReferenceCapturer();
	
	public static String toString(Object o) {
		String result;
		if (o instanceof AccessorByMember) {
			Member member = ((AccessorByMember) o).getGetter();
			if (member instanceof Method) {
				result = Reflections.toString((Method) member);
			} else {
				result = Reflections.toString((Field) member);
			}
		} else if (o instanceof AccessorByMethodReference) {
			result = MethodReferences.toMethodReferenceString(((AccessorByMethodReference) o).getMethodReference());
		} else if (o instanceof MutatorByMember) {
			Member member = ((MutatorByMember) o).getSetter();
			result = member.getName();
		} else if (o instanceof MutatorByMethodReference) {
			result = MethodReferences.toMethodReferenceString(((AccessorByMethodReference) o).getMethodReference());
		} else if (o instanceof PropertyAccessor) {
			IAccessor accessor = ((PropertyAccessor) o).getAccessor();
			result = toString(accessor);
		} else if (o instanceof AccessorChain) {
			StringAppender chainPrint = new StringAppender();
			((AccessorChain) o).getAccessors().iterator().forEachRemaining(a -> chainPrint.cat(toString(a)).cat(" > "));
			result = chainPrint.cutTail(3).toString();
		} else {
			throw new UnsupportedOperationException("Don't know how find out member definition for " + Reflections.toString(o.getClass()));
		}
		return result;
	}
	
	
	public static AccessComparator accessComparator() {
		return accessComparator(new HashMap<>());
	}
	
	public static AccessComparator accessComparator(Map<Object, MemberDefinition> cache) {
		return new AccessComparator(cache);
	}
	
	static class AccessComparator implements Comparator<Object> {
		
		/** Since {@link MemberDefinition} computation can be costly we use a cache, it may be shared between instances */
		private final Map<Object, MemberDefinition> cache;
		
		private AccessComparator(Map<Object, MemberDefinition> cache) {
			this.cache = cache;
		}
		
		@Override
		public int compare(Object o1, Object o2) {
			MemberDefinition memberDefinition1 = cache.computeIfAbsent(o1, AccessorChainComparator::giveMemberDefinition);
			MemberDefinition memberDefinition2 = cache.computeIfAbsent(o2, AccessorChainComparator::giveMemberDefinition);
			return memberDefinition1.compareTo(memberDefinition2);
		}
	}
	
	public static MemberDefinition giveMemberDefinition(Object o) {
		MemberDefinition result;
		if (o instanceof AbstractReflector) {
			result = giveMemberDefinition((AbstractReflector) o);
		} else if (o instanceof PropertyAccessor) {
			result = giveMemberDefinition((AbstractReflector) ((PropertyAccessor) o).getAccessor());
		} else if (o instanceof AccessorChain) {
			result = giveMemberDefinition((AccessorChain) o);
		} else {
			throw new UnsupportedOperationException("Don't know how find out member definition for " + Reflections.toString(o.getClass()));
		}
		return result;
	}
	
	public static MemberDefinition giveMemberDefinition(AbstractReflector o) {
		String memberName = null;
		Class declarator = null;
		Class memberType = null;
		if (o instanceof AccessorByMember) {
			Member member = ((AccessorByMember) o).getGetter();
			memberName = member.getName();
			declarator = member.getDeclaringClass();
			if (o instanceof AccessorByMethod) {
				Method getter = ((AccessorByMethod) o).getGetter();
				memberType = getter.getReturnType();
				try {
					memberName = Reflections.propertyName(getter);
				} catch (MemberNotFoundException e) {
					// method is not a Java bean standard name
					// what TO DO ???
				}
			} else {
				// AccessorByField case
				memberType = ((Field) member).getType();
			}
		}
		if (o instanceof AccessorByMethodReference) {
			Method method = METHOD_REFERENCE_CAPTURER.findMethod(((AccessorByMethodReference) o).getMethodReference());
			memberType = method.getReturnType();
			
			memberName = Reflections.propertyName(((AccessorByMethodReference) o).getMethodName());
			try {
				declarator = Reflections.forName(((AccessorByMethodReference) o).getDeclaringClass());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		if (o instanceof MutatorByMember) {
			Member member = ((MutatorByMember) o).getSetter();
			memberName = member.getName();
			declarator = member.getDeclaringClass();
			if (o instanceof MutatorByMethod) {
				Method setter = ((MutatorByMethod) o).getSetter();
				memberType = setter.getParameterTypes()[0];
				try {
					memberName = Reflections.propertyName(setter);
				} catch (MemberNotFoundException e) {
					// method is not a Java bean standard name
					// what TO DO ???
				}
			} else {
				// MutatorByField case
				memberType = ((Field) member).getType();
			}
		}
		
		if (o instanceof MutatorByMethodReference) {
			Method method = METHOD_REFERENCE_CAPTURER.findMethod(((MutatorByMethodReference) o).getMethodReference());
			memberType = method.getParameterTypes()[0];
			
			memberName = Reflections.propertyName(((MutatorByMethodReference) o).getMethodName());
			try {
				declarator = Reflections.forName(((MutatorByMethodReference) o).getDeclaringClass());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		return new MemberDefinition(declarator, memberName, memberType);
	}
	
	
	public static MemberDefinition giveMemberDefinition(AccessorChain o) {
		StringAppender stringAppender = new StringAppender() {
			@Override
			public StringAppender cat(Object s) {
				if (s instanceof AbstractReflector | s instanceof PropertyAccessor) {
					return super.cat(giveMemberDefinition(s).getName());
				} else {
					return super.cat(s);
				}
			}
		};
		stringAppender.ccat(o.getAccessors(), ".");
		Object firstAccessor = Iterables.first(o.getAccessors());
		Object lastAccessor = Iterables.last(o.getAccessors());
		return new MemberDefinition(
				giveMemberDefinition(firstAccessor).getDeclaringClass(),
				stringAppender.toString(),
				giveMemberDefinition(lastAccessor).getMemberType()
		);
	}
	
	public static class MemberDefinition implements Comparable<MemberDefinition> {
		
		private final Class declaringClass;
		private final String name;
		private final Class memberType;
		
		private MemberDefinition(Class declaringClass, String name, Class memberType) {
			this.declaringClass = declaringClass;
			this.name = name;
			this.memberType = memberType;
		}
		
		public Class getDeclaringClass() {
			return declaringClass;
		}
		
		public String getName() {
			return name;
		}
		
		@Override
		public int compareTo(@Nonnull MemberDefinition o) {
			if (name.equals(o.name)) {
				ClassIterator classIterator1 = new ClassIterator(declaringClass);
				ClassIterator classIterator2 = new ClassIterator(o.declaringClass);
				List<Class> copy1 = Iterables.copy(classIterator1);
				copy1.remove(Object.class);
				List<Class> copy2 = Iterables.copy(classIterator2);
				copy2.remove(Object.class);
				return Iterables.intersect(copy1, copy2).isEmpty() ? Integer.MIN_VALUE : 0;
			} else {
				return name.compareTo(o.name);
			}
		}
		
		public Class getMemberType() {
			return memberType;
		}
	}
	
//	public static List<String> xx(Set<AccessorChain> accessorChains) {
//		List<String> result = new ArrayList<>();
//		
//		class Node {
//			
//			private final String path;
//			private final Set<Node> children = new HashSet<>();
//			
//			Node(String path) {
//				this.path = path;
//			}
//			
//			private void addChild(Node node) {
//				children.add(node);
//			}
//			
//			private Node addIfAbsent(Node node, Set<Node> destination) {
//				Node foundNode = null;
//				Iterator<Node> childrenIterator = destination.iterator();
//				while(childrenIterator.hasNext() && foundNode == null) {
//					Node currentNode = childrenIterator.next();
//					if (currentNode.path.equals(node.path)) {
//						foundNode = currentNode;
//					}
//				}
//				// return added
//				return foundNode;
//			}
//			
//			@Override
//			public String toString() {
//				return path;
//			}
//		}
//		
//		Set<Node> roots = new HashSet<>();
//		for (AccessorChain accessorChain : accessorChains) {
//			Iterable<IAccessor> iterator = () -> Iterables.reverseIterator(accessorChain.getAccessors());
//			Set<Node>[] currentChildren = new Set[] { roots };
//			for (IAccessor accessor : iterator) {
//				Node nodeForCurrentAccessor = new Node(giveMemberDefinition(accessor).getName());
//				Node node = new Node("").addIfAbsent(nodeForCurrentAccessor, currentChildren[0]);
//				if (node != null) {
//					// node found, we go deeper in the existing node
//					currentChildren[0] = node.children;
//				} else {
//					// node doesn't exists, we add it to the current node, and we go deeper in it because its path doesn't exist
//					currentChildren[0].add(nodeForCurrentAccessor);
//					currentChildren[0] = nodeForCurrentAccessor.children;
//				}
//			}
//		}
//		
//		System.out.println(roots);
//		
//		return result;
//	}
}
