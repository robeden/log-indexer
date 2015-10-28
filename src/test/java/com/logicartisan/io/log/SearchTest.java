package com.logicartisan.io.log;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 *
 */
public class SearchTest {
	private LogIndexer<File> indexer = null;


	@After
	public void tearDown() {
		if ( indexer != null ) {
			indexer.close();
			indexer = null;
		}
	}


	/**
	 * Real world test case of search for "System" in "boot.log" (in resources). Original
	 * problem was failure to detect the search was complete.
	 */
	@Test
	public void testBootLogSearchForSystem() throws Exception {
		File boot_log_file =
			Paths.get( SearchTest.class.getResource( "boot.log" ).toURI() ).toFile();

		final CountDownLatch initial_index_complete_latch = new CountDownLatch( 1 );
		LogIndexListener<File> listener = new LogIndexListener<File>() {
			@Override
			public void indexingStarting( File attachment, boolean full ) {}

			@Override
			public void indexingFinished( File attachment, int total_rows ) {
				initial_index_complete_latch.countDown();
			}
		};


		LogIndexer<File> indexer = new LogIndexer<>( boot_log_file, boot_log_file,
			listener, 1000, 100, null );

		assertTrue( "Timed out waiting for initial index",
			initial_index_complete_latch.await( 5, TimeUnit.SECONDS ) );

		SearchParams params = new SearchParams.SimpleSearchParams( "system", false );
		final AtomicInteger matches_count = new AtomicInteger( 0 );
		final CountDownLatch search_complete_latch = new CountDownLatch( 1 );
		SearchListener search_listener = new SearchListener() {
			@Override
			public void searchScanFinished( int search_id,
				boolean exceed_max_matches ) {

				search_complete_latch.countDown();
			}

			@Override
			public void searchTermMatches( int search_id,
				SearchMatch... matches ) {

				matches_count.addAndGet( matches.length );
			}
		};
		indexer.startSearch( params, search_listener );

		assertTrue( "Timed out waiting for search completion",
			search_complete_latch.await( 5, TimeUnit.SECONDS ) );
		assertEquals( 59, matches_count.get() );
	}
}
