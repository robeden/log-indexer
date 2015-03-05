package com.logicartisan.io.log;

/**
 *
 */
public interface LogIndexListener<A> {
	/**
	 * Indicates an index is starting.
	 *
	 * @param attachment    Attachment associated with the {@link LogAccess} instance.
	 * @param full          Whether or not a full index is being run.
	 */
	public void indexingStarting( A attachment, boolean full );

	/**
	 * Indicates an index is finished.
	 *
	 * @param attachment    Attachment associated with the {@link LogAccess} instance.
	 * @param total_rows    Number of rows found in the file.
	 */
	public void indexingFinished( A attachment, int total_rows );
}
