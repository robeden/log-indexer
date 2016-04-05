package com.logicartisan.io.log;

import org.junit.After;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
	 * Real world test case of search for "COMMAND" in "boot.log" (in resources). Original
	 * problem was failure to detect the search was complete.
	 */
	@Test
	public void testOpenLMILogSearchForCommand() throws Exception {
		final List<SearchMatch> expected = new LinkedList<>();
		expected.add( new SearchMatch( 1, 20, 7 ) );
		expected.add( new SearchMatch( 11, 20, 7 ) );
		expected.add( new SearchMatch( 13, 20, 7 ) );
		expected.add( new SearchMatch( 15, 20, 7 ) );
		expected.add( new SearchMatch( 21, 20, 7 ) );


		SearchParams params = new SearchParams.SimpleSearchParams( "COMMAND", false );

		// NOTE: this log has a blank line in it, which threw off
		testBundleLogFile( "openlmi-install.log", params, expected );
	}


	/**
	 * Real world test case of search for "System" in "boot.log" (in resources). Original
	 * problem was failure to detect the search was complete.
	 */
	@Test
	public void testBootLogSearchForSystem() throws Exception {
		final List<SearchMatch> expected = new LinkedList<>();
		expected.add( new SearchMatch( 2, 39, 6 ) );
		expected.add( new SearchMatch( 4, 23, 6 ) );
		expected.add( new SearchMatch( 5, 0, 6 ) );
		expected.add( new SearchMatch( 5, 44, 6 ) );
		expected.add( new SearchMatch( 6, 31, 6 ) );
		expected.add( new SearchMatch( 9, 45, 6 ) );
		expected.add( new SearchMatch( 10, 45, 6 ) );
		expected.add( new SearchMatch( 12, 50, 6 ) );
		expected.add( new SearchMatch( 15, 45, 6 ) );
		expected.add( new SearchMatch( 22, 45, 6 ) );
		expected.add( new SearchMatch( 23, 23, 6 ) );
		expected.add( new SearchMatch( 24, 31, 6 ) );
		expected.add( new SearchMatch( 25, 50, 6 ) );
		expected.add( new SearchMatch( 29, 32, 6 ) );
		expected.add( new SearchMatch( 33, 32, 6 ) );
		expected.add( new SearchMatch( 37, 29, 6 ) );
		expected.add( new SearchMatch( 38, 74, 6 ) );
		expected.add( new SearchMatch( 40, 43, 6 ) );
		expected.add( new SearchMatch( 41, 34, 6 ) );
		expected.add( new SearchMatch( 60, 23, 6 ) );
		expected.add( new SearchMatch( 61, 37, 6 ) );
		expected.add( new SearchMatch( 62, 51, 6 ) );
		expected.add( new SearchMatch( 63, 42, 6 ) );
		expected.add( new SearchMatch( 71, 0, 6 ) );
		expected.add( new SearchMatch( 71, 44, 6 ) );
		expected.add( new SearchMatch( 72, 31, 6 ) );
		expected.add( new SearchMatch( 73, 47, 6 ) );
		expected.add( new SearchMatch( 74, 55, 6 ) );
		expected.add( new SearchMatch( 78, 40, 6 ) );
		expected.add( new SearchMatch( 82, 44, 6 ) );
		expected.add( new SearchMatch( 83, 41, 6 ) );
		expected.add( new SearchMatch( 86, 49, 6 ) );
		expected.add( new SearchMatch( 99, 23, 6 ) );
		expected.add( new SearchMatch( 100, 0, 6 ) );
		expected.add( new SearchMatch( 100, 44, 6 ) );
		expected.add( new SearchMatch( 101, 31, 6 ) );
		expected.add( new SearchMatch( 104, 23, 6 ) );
		expected.add( new SearchMatch( 106, 0, 6 ) );
		expected.add( new SearchMatch( 106, 44, 6 ) );
		expected.add( new SearchMatch( 107, 31, 6 ) );
		expected.add( new SearchMatch( 128, 44, 6 ) );
		expected.add( new SearchMatch( 137, 36, 6 ) );
		expected.add( new SearchMatch( 138, 44, 6 ) );
		expected.add( new SearchMatch( 139, 33, 6 ) );
		expected.add( new SearchMatch( 149, 37, 6 ) );
		expected.add( new SearchMatch( 151, 39, 6 ) );
		expected.add( new SearchMatch( 155, 25, 6 ) );
		expected.add( new SearchMatch( 162, 18, 6 ) );
		expected.add( new SearchMatch( 172, 26, 6 ) );
		expected.add( new SearchMatch( 185, 24, 6 ) );
		expected.add( new SearchMatch( 186, 32, 6 ) );
		expected.add( new SearchMatch( 189, 33, 6 ) );
		expected.add( new SearchMatch( 196, 26, 6 ) );
		expected.add( new SearchMatch( 207, 34, 6 ) );
		expected.add( new SearchMatch( 230, 45, 6 ) );
		expected.add( new SearchMatch( 231, 45, 6 ) );
		expected.add( new SearchMatch( 245, 36, 6 ) );
		expected.add( new SearchMatch( 246, 44, 6 ) );
		expected.add( new SearchMatch( 248, 5, 6 ) );

		SearchParams params = new SearchParams.SimpleSearchParams( "system", false );

		testBundleLogFile( "boot.log", params, expected );
	}


	private void testBundleLogFile( String file_name, SearchParams params,
		List<SearchMatch> expected ) throws Exception {

		final int expected_match_count = expected.size();

		File file =
			Paths.get( SearchTest.class.getResource( file_name ).toURI() ).toFile();

		final CountDownLatch initial_index_complete_latch = new CountDownLatch( 1 );
		LogIndexListener<File> listener = new LogIndexListener<File>() {
			@Override
			public void indexingStarting( File attachment, boolean full ) {}

			@Override
			public void indexingFinished( File attachment, int total_rows ) {
				initial_index_complete_latch.countDown();
			}
		};


		LogIndexer<File> indexer = new LogIndexer<>( file, file,
			listener, 1000, 100, null );

		assertTrue( "Timed out waiting for initial index",
			initial_index_complete_latch.await( 5, TimeUnit.SECONDS ) );

		final AtomicInteger matches_count = new AtomicInteger( 0 );
		final CountDownLatch search_complete_latch = new CountDownLatch( 1 );


		AtomicReference<String> error_slot = new AtomicReference<>();
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

				for( SearchMatch match : matches ) {
					if ( expected.isEmpty() ) {
						error_slot.set( "No more terms expected: " + match );
						continue;
					}

					SearchMatch expected_match = expected.remove( 0 );
					if ( !expected_match.equals( match ) ) {
						error_slot.set( "Expected: " + expected_match +
							"\nFound: " + match );
					}
				}
			}
		};
		indexer.startSearch( params, search_listener );

		assertTrue( "Timed out waiting for search completion",
			search_complete_latch.await( 5, TimeUnit.SECONDS ) );
		assertEquals( expected_match_count, matches_count.get() );

		assertNull( error_slot.get() );
		assertTrue( expected.toString(), expected.isEmpty() );



		// Read every line and make sure they're as expected
		int index = 0;
		try( BufferedReader in = new BufferedReader( new FileReader( file ) ) ) {
			String line;
			while( ( line = in.readLine() ) != null ) {
				String[] lines = indexer.readLines( index, 1 );
				assertEquals( "Mismatch at index " + index, line, lines[ 0 ] );

				System.out.println( "Line " + index + ": " + line );
				index++;
			}
		}
		// NOTE: Adding one to the index because BufferedReader doesn't count the final
		//       line, which is simply a blank line
		assertEquals( indexer.getLineCount(), index );
	}
}
