package org.codefilarete.stalactite.sql.spring.repository.query;

import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParameterOutOfBoundsException;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;

/**
 * Validator for query method. Basically, it ensures that arguments match length and types of properties.
 *
 * @author Guillaume Mary
 * @see #validate()
 */
public class QueryMethodValidator {
	
	private final PartTree tree;
	private final QueryMethod method;
	
	public QueryMethodValidator(PartTree tree, QueryMethod method) {
		this.tree = tree;
		this.method = method;
	}
	
	public void validate() {
		int argCount = 0;
		Iterable<Part> parts = () -> tree.stream().flatMap(Streamable::stream).iterator();
		for (Part part : parts) {
			int numberOfArguments = part.getNumberOfArguments();
			for (int i = 0; i < numberOfArguments; i++) {
				throwExceptionOnArgumentMismatch(part, argCount);
				argCount++;
			}
		}
	}
	
	private void throwExceptionOnArgumentMismatch(Part part, int index) {
		Type type = part.getType();
		String property = part.getProperty().toDotPath();
		
		Parameter parameter;
		try {
			parameter = method.getParameters().getBindableParameter(index);
		} catch (ParameterOutOfBoundsException e) {
			throw new IllegalStateException(String.format(
					"Method %s expects at least %d arguments but only found %d. This leaves an operator of type %s for property %s unbound.",
					method.getName(), index + 1, index, type.name(), property));
		}
		
		if (expectsCollection(type) && !parameterIsCollectionLike(parameter)) {
			throw new IllegalStateException(wrongParameterTypeMessage(property, type, "Collection", parameter));
		} else if (!expectsCollection(type) && !parameterIsScalarLike(parameter)) {
			throw new IllegalStateException(wrongParameterTypeMessage(property, type, "scalar", parameter));
		}
	}
	
	private String wrongParameterTypeMessage(String property, Type operatorType, String expectedArgumentType, Parameter parameter) {
		
		return String.format("Operator %s on %s requires a %s argument, found %s in method %s.", operatorType.name(),
				property, expectedArgumentType, parameter.getType(), method.getName());
	}
	
	private boolean parameterIsCollectionLike(Parameter parameter) {
		return Iterable.class.isAssignableFrom(parameter.getType()) || parameter.getType().isArray();
	}
	
	/**
	 * Arrays are may be treated as collection like or in the case of binary data as scalar
	 */
	private boolean parameterIsScalarLike(Parameter parameter) {
		return !Iterable.class.isAssignableFrom(parameter.getType());
	}
	
	private boolean expectsCollection(Type type) {
		return type == Type.IN || type == Type.NOT_IN;
	}
}
