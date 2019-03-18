package org.gama.stalactite.persistence.id.provider;

import java.util.function.Supplier;

import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * Implementation based on {@link Supplier}
 * 
 * @author Guillaume Mary
 */
public class IdentifierSupplier<T> implements IdentifierProvider<T> {
	
	private final Supplier<T> surrogateSupplier;
	
	public IdentifierSupplier(Supplier<T> surrogateSupplier) {
		this.surrogateSupplier = surrogateSupplier;
	}
	
	@Override
	public final PersistableIdentifier<T> giveNewIdentifier() {
		return new PersistableIdentifier<>(surrogateSupplier.get());
	}
}
