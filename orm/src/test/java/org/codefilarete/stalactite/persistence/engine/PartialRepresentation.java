package org.codefilarete.stalactite.persistence.engine;

import org.assertj.core.presentation.Representation;
import org.assertj.core.presentation.StandardRepresentation;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.trace.ObjectPrinterBuilder.ObjectPrinter;

/**
 * An AssertJ representation that uses {@link ObjectPrinter} to print objects
 * 
 * @author Guillaume Mary
 */
class PartialRepresentation<T> implements Representation {
	
	private final Class<T> managedType;
	private final ObjectPrinter<T> printer;
	
	public PartialRepresentation(Class<T> managedType, ObjectPrinter<T> printer) {
		this.managedType = managedType;
		this.printer = printer;
	}
	
	@Override
	public String toStringOf(Object object) {
		if (object instanceof Iterable) {
			StringAppender result = new StringAppender(); 
			for (Object o : ((Iterable) object)) {
				String representation;
				if (o == null) {
					representation = "null";
				} else if (managedType.isInstance(o)) {
					representation = printer.toString((T) o);
				} else {
					representation = new StandardRepresentation().toStringOf(object);
				}
				result.cat(representation, ",\r\n");
			}
			return result.cutTail(3).wrap("{", "}").toString();
		} else {
			return printer.toString((T) object);
		}
	}
	
	@Override
	public String unambiguousToStringOf(Object object) {
		return object.getClass() + "@" + Integer.toHexString(System.identityHashCode(object));
	}
}
