package org.codefilarete.stalactite.spring.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Stalactite
 * 
 * @author Guillaume Mary
 */
@ConfigurationProperties("spring.stalactite")
public class StalactiteProperties {
	
	/**
	 * DDL mode. This is actually a shortcut for the "hibernate.hbm2ddl.auto" property.
	 * Defaults to "create-drop" when using an embedded database and no schema manager was
	 * detected. Otherwise, defaults to "none".
	 */
	private String ddlAuto;
}