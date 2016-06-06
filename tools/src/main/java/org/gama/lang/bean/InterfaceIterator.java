package org.gama.lang.bean;

/**
 * @author Guillaume Mary
 */
public class InterfaceIterator extends InheritedElementIterator<Class> {
	
	public InterfaceIterator(Class aClass) {
		this(new ClassIterator(aClass));
	}
	
	public InterfaceIterator(ClassIterator classIterator) {
		super(classIterator);
	}
	
	@Override
	protected Class[] getElements(Class clazz) {
		return clazz.getInterfaces();
	}
}
