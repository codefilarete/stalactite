package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.engine.configurer.resolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;

public class RelationsMetadataResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public RelationsMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	<C, I> void resolve(EntitySource<C, I> entityHierarchy) {
		OneToOneMetadataResolver oneToOneMetadataResolver = new OneToOneMetadataResolver(dialect, connectionConfiguration);
		oneToOneMetadataResolver.resolve(entityHierarchy);
	}
}
