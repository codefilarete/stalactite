package org.codefilarete.stalactite.engine.idprovider;

import java.util.function.Supplier;

/**
 * Implementation based on {@link Supplier}
 * 
 * @author Guillaume Mary
 */
public class IdentifierSupplier<T> implements IdentifierProvider<T> {
	
	private final Supplier<T> delegateSupplier;
	
	public IdentifierSupplier(Supplier<T> delegateSupplier) {
		this.delegateSupplier = delegateSupplier;
	}
	
	@Override
	public final T giveNewIdentifier() {
		return delegateSupplier.get();
	}
}
