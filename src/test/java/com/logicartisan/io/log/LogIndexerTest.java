/*
 * Copyright (c) 2013 Rob Eden.
 * All Rights Reserved.
 */

package com.logicartisan.io.log;

import com.starlight.IOKit;
import com.starlight.io.log.LogIndexListener;
import com.starlight.io.log.LogIndexer;
import com.starlight.io.log.SearchListener;
import com.starlight.io.log.SearchMatch;
import com.starlight.io.log.SearchParams;
import com.starlight.thread.ThreadKit;
import junit.framework.TestCase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.easymock.EasyMock.*;


/**
 *
 */
public class LogIndexerTest extends TestCase {
	private File file;
	private PrintWriter writer;

	private LogIndexer<File> indexer;


	@Override
	protected void setUp() throws Exception {
		file = File.createTempFile( LogIndexerTest.class.getSimpleName() + "-", ".test" );
		file.deleteOnExit();

		writer = new PrintWriter( new FileWriter( file, false ) );
	}

	@Override
	protected void tearDown() throws Exception {
		if ( indexer != null ) indexer.close();

		//noinspection ResultOfMethodCallIgnored
		file.delete();
		IOKit.close( writer );
	}



	public void testIndexing() throws Exception {
		writeLines( "Test line 1", "Test line 2", "Test line 3" );

		//noinspection unchecked
		LogIndexListener<File> mock_listener = createMock( LogIndexListener.class );
		mock_listener.indexingStarting( file, true );
		mock_listener.indexingFinished( file, 3 );
		replay( mock_listener );

		System.out.println( "Indexer file: " + file );
		indexer = new LogIndexer<>( file, file, mock_listener, 2000, 100, null );

		ThreadKit.sleep( 2000 );
		verify( mock_listener );

		String[] lines = indexer.readLines( 0, 5 );
		assertEquals( 5, lines.length );
		assertEquals( "Test line 1", lines[ 0 ] );
		assertEquals( "Test line 2", lines[ 1 ] );
		assertEquals( "Test line 3", lines[ 2 ] );
		assertNull( lines[ 3 ] );
		assertNull( lines[ 4 ] );


		// No changes
		reset( mock_listener );
		replay( mock_listener );
		ThreadKit.sleep( 3000 );
		verify( mock_listener );


		// Add a line
		reset( mock_listener );
		mock_listener.indexingStarting( file, false );
		mock_listener.indexingFinished( file, 4 );
		replay( mock_listener );

		writeLines( "Test line 4" );

		ThreadKit.sleep( 3000 );
		verify( mock_listener );

		lines = indexer.readLines( 0, 5 );
		assertEquals( 5, lines.length );
		assertEquals( "Test line 1", lines[ 0 ] );
		assertEquals( "Test line 2", lines[ 1 ] );
		assertEquals( "Test line 3", lines[ 2 ] );
		assertEquals( "Test line 4", lines[ 3 ] );
		assertNull( lines[ 4 ] );
	}


	public void testSearching_SimpleSensitive() throws IOException, InterruptedException {
		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 9, 3 ),
			new SearchMatch( 1, 20, 3 ),
			new SearchMatch( 1, 35, 3 ),
			new SearchMatch( 4, 0, 3 ) };
		SearchMatch[] second = new SearchMatch[]{ new SearchMatch( 6, 12, 3 ) };

		doTestSearching( com.starlight.io.log.SearchParams.createSimple( "hat", true ), first, second,
			1000, false );
	}

	public void testSearching_SimpleInsensitive()
		throws IOException, InterruptedException {

		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 0, 3 ),
			new SearchMatch( 1, 9, 3 ),
			new SearchMatch( 1, 20, 3 ),
			new SearchMatch( 1, 35, 3 ),
			new SearchMatch( 3, 0, 3 ),
			new SearchMatch( 4, 0, 3 ) };
		SearchMatch[] second = new SearchMatch[]{ new SearchMatch( 6, 12, 3 ) };

		doTestSearching( com.starlight.io.log.SearchParams.createSimple( "hat", false ), first, second,
			1000, false );
	}

	// Same as testSearching_SimpleSensitive
	public void testSearching_Regex1() throws IOException, InterruptedException {
		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 9, 3 ),
			new SearchMatch( 1, 20, 3 ),
			new SearchMatch( 1, 35, 3 ),
			new SearchMatch( 4, 0, 3 ) };
		SearchMatch[] second = new SearchMatch[]{ new SearchMatch( 6, 12, 3 ) };

		doTestSearching( com.starlight.io.log.SearchParams
			.createRegex( Pattern.compile( "hat" ) ),
			first, second, 1000, false );
	}

	public void testSearching_maxHits() throws IOException, InterruptedException {
		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 0, 3 ),
			new SearchMatch( 1, 9, 3 ) };

		doTestSearching( com.starlight.io.log.SearchParams.createSimple( "hat", false ), first, null,
			2, true );
	}


	private void doTestSearching( SearchParams params, SearchMatch[] first_match_set,
		SearchMatch[] second_match_set, final int max_search_hits,
		final boolean should_exceed_max_matches )
		throws IOException, InterruptedException {

		writeLines( "Some line with no matches",
			"Hate to chat about that display of hats.",
//                    ---        ---            ---
//           0123456789012345678901234567890123456789
//           0         1         2         3
			"Again, nothing to match",
			"Hat",
			"hat" );

		LogIndexListener<File> do_nothin = new LogIndexListener<File>() {
			@Override
			public void indexingStarting( File file, boolean full ) {}

			@Override
			public void indexingFinished( File file, int total_rows ) {}
		};

		indexer = new LogIndexer<>( file, file, do_nothin, 2000, max_search_hits, null );
		System.out.println( "Indexer is: " + indexer );

		final AtomicBoolean has_failure = new AtomicBoolean( false );
		final AtomicReference<CountDownLatch> matches_latch = new AtomicReference<>(
			new CountDownLatch( 1 ) );
		final CountDownLatch finished_latch = new CountDownLatch( 1 );
		final AtomicReference<SearchMatch[]> expect_match = new AtomicReference<>();
		expect_match.set( first_match_set );
		SearchListener listener = new SearchListener() {
			@Override
			public void searchScanFinished( int search_id, boolean exceed_max_matches ) {
//				System.out.println( "Latch for listener (" + this + "): " +
//					finished_latch.get() );
				System.out.println( "searchScanFinished(" + search_id + "," +
					exceed_max_matches + ")" );
				if ( finished_latch.getCount() == 0 ) {
//					System.err.println( "Duplicate call to searchScanFinished" );
//					has_failure.set( true );
				}

				if ( should_exceed_max_matches != exceed_max_matches ) {
					System.err.println( "Unexpected value for exceed_max_matches: " +
						exceed_max_matches + " (expected " + should_exceed_max_matches +
						")" );
					has_failure.set( true );
				}

				finished_latch.countDown();
			}

			@Override
			public void searchTermMatches( int search_id, SearchMatch... matches ) {
				System.out.println( "searchTermMatches(" + search_id + "," +
					Arrays.toString( matches ) + ")" );
				if ( matches_latch.get().getCount() == 0 ) {
					System.err.println( "Unexpected call to searchTermMatches: " +
						Arrays.toString( matches ) );
					has_failure.set( true );
				}
				if ( Arrays.equals( expect_match.get(), matches ) ) {
					matches_latch.get().countDown();
				}
				else {
					System.err.println( "Unexpected array contents in searchTermMatches:" );
					for( SearchMatch match : matches ) {
						System.err.println( "  " + match );
					}
					has_failure.set( true );
				}
			}
		};

		indexer.startSearch( params, listener );

		assertTrue( matches_latch.get().await( 5, TimeUnit.SECONDS ) );
		assertTrue( finished_latch.await( 5, TimeUnit.SECONDS ) );
		assertFalse( has_failure.get() );


		// Add a line - non matching
		// NOTE: Latches are already counted down, so another call would be a failure

		writeLines( "This shouldn't match" );

		ThreadKit.sleep( 2000 );
		assertFalse( has_failure.get() );


		// Add a line - matching

		// NOTE: don't expect another searchScanFinished call, so not changing latch
		matches_latch.set( new CountDownLatch( 1 ) );
		expect_match.set( second_match_set );

		writeLines( "I can haz a hat" );
		//           012345678901234
		//           0         1

		matches_latch.get().await( 2, TimeUnit.SECONDS );
		ThreadKit.sleep( 2000 );
		assertFalse( has_failure.get() );
	}


	private void writeLines( String... lines ) throws IOException {
		for( String line : lines ) {
			writer.println( line );
		}
		writer.flush();
	}
}