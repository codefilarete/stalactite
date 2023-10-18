package org.codefilarete.stalactite.sql.spring.repository.config;

import org.springframework.instrument.classloading.ShadowingClassLoader;

/**
 * Disposable {@link ClassLoader} used to inspect user-code classes within an isolated class loader without preventing
 * class transformation at a later time.
 *
 * @author Guillaume Mary
 */
class InspectionClassLoader extends ShadowingClassLoader {

	/**
	 * Create a new {@link InspectionClassLoader} instance.
	 *
	 * @param parent the parent classloader.
	 */
	InspectionClassLoader(ClassLoader parent) {

		super(parent, true);

		excludePackage("org.springframework.");
	}
}
