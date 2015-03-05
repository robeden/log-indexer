package com.logicartisan.io.log;

/**
 *
 */
public interface SearchListener {
	/**
	 * Indicates that a search has finished scanning existing contents of a file.
	 *
	 * The search will continue to run until cancelled, so additional matches may be
	 * found.
	 *
	 * @param search_id             ID of the search.
	 * @param exceed_max_matches    True if the search terminated due to excessive
	 *                              matches.
	 */
	public void searchScanFinished( int search_id, boolean exceed_max_matches );

	/**
	 * Indicates a new batch of matches for the active search term.
	 *
	 * @param search_id     ID of the search.
	 */
	public void searchTermMatches( int search_id, SearchMatch... matches );
}
