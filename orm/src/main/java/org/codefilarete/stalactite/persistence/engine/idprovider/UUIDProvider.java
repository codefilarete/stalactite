package org.codefilarete.stalactite.persistence.engine.idprovider;

import java.util.UUID;

/**
 * Simple implementation that uses {@link UUID} to generate an identifier.
 * 
 * Thread-safe because {@link UUID#randomUUID()} is.
 * 
 * @author Guillaume Mary
 */
public final class UUIDProvider extends IdentifierSupplier<String> {
	
	public UUIDProvider() {
		super(() -> UUID.randomUUID().toString());
	}
}
