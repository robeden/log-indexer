/*
 * Copyright (c) 2013 Rob Eden.
 * All Rights Reserved.
 */

package com.logicartisan.io.log;

import java.io.IOException;


/**
 *
 */
public interface LogAccess<A> {

	/**
	 * Returns the attachment associated with this instance.
	 */
	public A getAttachment();


	/**
	 * Retrieve a number of lines. Any lines that are unavailable will be set to null.
	 *
	 * @param start     The starting line index (zero-based).
	 * @param count     The number of lines to retrieve.
	 *
	 * @return  The array of lines. Will always be non-null and of length <tt>count</tt>.
	 * @throws IOException
	 */
	public String[] readLines( int start, int count ) throws IOException;


	/**
	 * Returns the current number of lines known in the file.
	 */
	public int getLineCount();


	/**
	 * Add a listener that is notified when additional indexing is done, such as when
	 * new lines are added to the file.
	 *
	 * @return      The current number of lines in the file.
	 */
	public int addListener( LogIndexListener<A> listener );

	/**
	 * Remove a listener.
	 */
	public void removeListener( LogIndexListener<A> listener );


	/**
	 * Starts a new search. Searches will continue to run until cancelled, or until
	 * communication errors occur to the listener.
	 *
	 * @param params        Parameters for the search.
	 * @param listener      The SearchListener that will be notified of search results.
	 *
	 * @return              The search ID.
	 */
	public int startSearch( SearchParams params, SearchListener listener );


	/**
	 * Cancels a currently running search.
	 */
	public void stopSearch( int search_id );


	/**
	 * Resumes a search that was halted or stopped after having reached the maximum number of matches
	 * allowed for a search. Resets tracked number of matches, allowing the resumed search to find
	 * another maximum number of matches.
	 *
	 * @param search_id     The ID for the search to resume
	 *
	 * @return              True if the search was resumed
	 */
	public boolean resumeSearch( int search_id );


	/**
	 * Sets the full/partial index handling for file changes.
	 * @param full_index_after_modified When true, full indexing of the file will occur
	 *                                  whenever the last modified time is updated. When
	 *                                  false, full indexing occurs when the file shrinks,
	 *                                  and partial indexing from the last indexed
	 *                                  position occurs when the file grows.
	 */
	public void setFullIndexAfterModified( boolean full_index_after_modified );
}
