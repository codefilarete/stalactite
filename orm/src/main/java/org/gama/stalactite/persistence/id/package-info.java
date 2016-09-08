/**
 * Package for persistent bean identifier.
 * {@link org.gama.stalactite.persistence.id.PersistedIdentifier} are expected to be used for already persisted but not freshly inserted beans (so
 * those that come from a select).
 * {@link org.gama.stalactite.persistence.id.PersistableIdentifier} are expected to be used for insertable (never persisted) beans.
 * 
 * @author Guillaume Mary
 */
package org.gama.stalactite.persistence.id;
