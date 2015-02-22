package org.stalactite.persistence.id.sequence;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.stalactite.persistence.id.IdentifierGenerator;
import org.stalactite.persistence.id.sequence.PooledSequencePersister.PooledSequence;
import org.stalactite.persistence.sql.ddl.DDLParticipant;

/**
 * Générateur d'identifiant Long par table/classe (une séquence par table).
 * Stocke l'état de toutes les séquences dans une table.
 * Chaque séquence peut avoir une taille de paquet différent.
 * Inspiré du mécanisme "enhanced table generator" avec Hilo Optimizer d'Hibernate.
 * 
 * @author mary
 */
public class PooledSequenceIdentifierGenerator implements IdentifierGenerator, DDLParticipant {
	
	private LongPool sequenceState;
	
	private PooledSequencePersister pooledSequencePersister;
	
	private PooledSequenceIdentifierGeneratorOptions options;
	
	public PooledSequenceIdentifierGenerator() {
	}
	
	public PooledSequenceIdentifierGenerator(PooledSequenceIdentifierGeneratorOptions options) {
		configure(options);
	}
	
	@Override
	public void configure(Map<String, Object> configuration) {
		configure(new PooledSequenceIdentifierGeneratorOptions(configuration));
	}
	
	public void configure(PooledSequenceIdentifierGeneratorOptions options) {
		this.pooledSequencePersister = new PooledSequencePersister(options.getStorageOptions());
		this.options = options;
	}
	
	@Override
	public List<String> getCreationScripts() {
		return pooledSequencePersister.getCreationScripts();
	}
	
	@Override
	public synchronized Serializable generate() {
		if (sequenceState == null) {
			PooledSequence existingSequence = this.pooledSequencePersister.select(getSequenceName());
			long initialValue = existingSequence == null ? 0 : existingSequence.getUpperBound();
			this.sequenceState = new LongPool(options.getPoolSize(), initialValue) {
				@Override
				void onBoundReached() {
					pooledSequencePersister.reservePool(getSequenceName(), getPoolSize());
				}
			};
			// if no sequence exist, should consider that's a new boundary reached (so insert initial state is done)
			if (existingSequence == null) {
				sequenceState.onBoundReached();
			}
		}
		return sequenceState.nextValue();
	}
	
	protected String getSequenceName() {
		return this.options.getSequenceName();
	}
	
	/**
	 * Représentation d'une plage d'incrément de long.
	 * Utilisé comme cache de long. Permet d'être prévenu quand la limite haute de la plage est atteinte.
	 */
	private static abstract class LongPool {
		/** Taille de la réserve, ne change pas au cours de la vie de cette instance */
		private final int poolSize;
		/** Valeur incrémentée */
		private long currentValue;
		/** Limite haute de la plage courante, est incrémentée de poolSize quand elle est atteinte */
		protected long upperBound;
		
		public LongPool(int poolSize, long currentValue) {
			this.poolSize = poolSize;
			this.currentValue = currentValue;
			nextBound();
		}
		
		public int getPoolSize() {
			return poolSize;
		}
		
		public long getCurrentValue() {
			return currentValue;
		}
		
		/**
		 * Renvoie la valeur suivante: valeur précédente + 1.
		 * Appelle {@link #onBoundReached()} si la limite du segment est atteinte.
		 * 
		 * @return le nombre entier suivant.
		 */
		public long nextValue() {
			currentValue++;
			if (currentValue == upperBound) {
				onBoundReached();
				nextBound();
			}
			return currentValue;
		}
		
		/**
		 * Méthode chargée de déplacer la limite haute: taille du segment + valeur courante
		 */
		protected void nextBound() {
			this.upperBound = currentValue + poolSize;
		}
		
		/**
		 * Méthode appelée quand la limite (haute) du segment est atteinte
		 */
		abstract void onBoundReached();
	}
}
