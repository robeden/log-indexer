/*
 * Copyright (c) 2013 Rob Eden.
 * All Rights Reserved.
 */

package com.logicartisan.io.log;

import com.logicartisan.common.core.IOKit;
import com.logicartisan.common.core.thread.ThreadKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.easymock.EasyMock.*;
import static org.junit.jupiter.api.Assertions.*;


/**
 *
 */
public class LogIndexerTest {
	private File file;
	private PrintWriter writer;

	private LogIndexer<File> indexer;


	@BeforeEach
	void setUp() throws Exception {
		file = File.createTempFile( LogIndexerTest.class.getSimpleName() + "-", ".test" );
	}

	@AfterEach
	void tearDown() {
		if ( indexer != null ) {
			indexer.close();
			indexer = null;
		}

		if( writer != null ) {
			IOKit.close( writer );
			writer = null;
		}

		//noinspection ResultOfMethodCallIgnored
		file.delete();
		file = null;
	}


	@Test
	public void testIndexing() throws Exception {

		writer = new PrintWriter( new FileWriter( file, false ) );

		writeLines( "Test line 1", "Test line 2", "Test line 3" );

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

	@Test
	public void testSearching_SimpleSensitive() throws IOException, InterruptedException {
		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 9, 3 ),
			new SearchMatch( 1, 20, 3 ),
			new SearchMatch( 1, 35, 3 ),
			new SearchMatch( 4, 0, 3 ) };
		SearchMatch[] second = new SearchMatch[]{ new SearchMatch( 6, 12, 3 ) };

		doTestSearching( SearchParams.createSimple( "hat", true ), first, second,
			1000, false );
	}

	@Test
	public void testSearching_SimpleInsensitive()
		throws IOException, InterruptedException {

		System.out.println( "----------------------------------------------------------" );
		System.err.println( "----------------------------------------------------------" );

		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 0, 3 ),
			new SearchMatch( 1, 9, 3 ),
			new SearchMatch( 1, 20, 3 ),
			new SearchMatch( 1, 35, 3 ),
			new SearchMatch( 3, 0, 3 ),
			new SearchMatch( 4, 0, 3 ) };
		SearchMatch[] second = new SearchMatch[]{ new SearchMatch( 6, 12, 3 ) };

		doTestSearching( SearchParams.createSimple( "hat", false ), first, second,
			1000, false );
	}

	@Test
	// Same as testSearching_SimpleSensitive
	public void testSearching_Regex1() throws IOException, InterruptedException {
		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 9, 3 ),
			new SearchMatch( 1, 20, 3 ),
			new SearchMatch( 1, 35, 3 ),
			new SearchMatch( 4, 0, 3 ) };
		SearchMatch[] second = new SearchMatch[]{ new SearchMatch( 6, 12, 3 ) };

		doTestSearching( SearchParams.createRegex( Pattern.compile( "hat" ) ),
			first, second, 1000, false );
	}

	@Test
	public void testSearching_maxHits() throws IOException, InterruptedException {

		System.out.println( "----------------------------------------------------------" );
		System.err.println( "----------------------------------------------------------" );

		SearchMatch[] first = new SearchMatch[] {
			new SearchMatch( 1, 0, 3 ),
			new SearchMatch( 1, 9, 3 ) };

		doTestSearching( SearchParams.createSimple( "hat", false ), first, null,
			2, true );
	}


	private void doTestSearching( SearchParams params, SearchMatch[] first_match_set,
		SearchMatch[] second_match_set, final int max_search_hits,
		final boolean should_exceed_max_matches )
		throws IOException, InterruptedException {

		writer = new PrintWriter( new FileWriter( file, false ) );

		writeLines( "Some line with no matches",
			"Hate to chat about that display of hats.",
//                    ---        ---            ---
//           0123456789012345678901234567890123456789
//           0         1         2         3
			"Again, nothing to match",
			"Hat",
			"hat" );

		LogIndexListener<File> do_nothin = new LogIndexListener<>() {
            @Override
            public void indexingStarting(File file, boolean full) {
            }

            @Override
            public void indexingFinished(File file, int total_rows) {
                System.out.println("Told of " + total_rows + " rows");
            }
        };

		indexer = new LogIndexer<>( file, file, do_nothin, 2000, max_search_hits, null );
		System.out.println( "Indexer is: " + indexer );

		final AtomicBoolean has_failure = new AtomicBoolean( false );
		final AtomicReference<CountDownLatch> matches_latch = new AtomicReference<>(
			new CountDownLatch( 1 ) );
		final CountDownLatch finished_latch = new CountDownLatch( 1 );
		final List<SearchMatch> expected_matches = new ArrayList<>( 10 );
		expected_matches.addAll( Arrays.asList( first_match_set ) );
		System.out.println( "Expecting first: " + expected_matches );

		SearchListener listener = new SearchListener() {
			@Override
			public void searchScanFinished( int search_id, boolean exceed_max_matches ) {
//				System.out.println( "Latch for listener (" + this + "): " +
//					finished_latch.get() );
				System.out.println( "searchScanFinished(" + search_id + "," +
					exceed_max_matches + ")" );

				if ( expected_matches.isEmpty() ) {
					if ( should_exceed_max_matches != exceed_max_matches ) {
						System.err.println( "Unexpected value for exceed_max_matches: " +
							exceed_max_matches + " (expected " + should_exceed_max_matches +
							")" );
						has_failure.set( true );
					}


					finished_latch.countDown();
				}
				else {
					System.out.println( "Told that search was finished when some " +
						"matches have not occurred. A search may start again though. " +
						"Not currently matched: " + expected_matches );
				}
			}

			@Override
			public void searchTermMatches( int search_id, SearchMatch... matches ) {
				System.out.println( "searchTermMatches(" + search_id + ",\n   " +
					Arrays.stream( matches )
						.map( Object::toString )
						.collect( Collectors.joining( "\n   " ) ) );

				if ( expected_matches.isEmpty() ) {
					System.err.println( "Unexpected call to searchTermMatches: " +
						Arrays.toString( matches ) );
					has_failure.set( true );
					return;
				}

				for( SearchMatch match : matches ) {
					if ( !expected_matches.remove( match ) ) {
						System.err.println( "Unexpected match in searchTermMatches: " +
							match + "  All returned matches: " +
							Arrays.toString( matches ) + "  Still expecting: " +
							expected_matches );
						has_failure.set( true );
					}
				}

				if ( expected_matches.isEmpty() ) {
					matches_latch.get().countDown();
				}
			}
		};

		indexer.startSearch( params, listener );

		assertTrue( matches_latch.get().await( 5, TimeUnit.SECONDS ) );
		assertTrue( finished_latch.await( 5, TimeUnit.SECONDS ) );
		assertFalse( has_failure.get() );

//		assertTrue( "Still in expecting list: " + expected_matches,
//			expected_matches.isEmpty() );

		// Add a line - non matching
		// NOTE: Latches are already counted down, so another call would be a failure

		System.out.println( "Writing additional lines to test file..." );
		writeLines( "This shouldn't match" );

		ThreadKit.sleep( 2000 );
		assertFalse( has_failure.get() );


		// Add a line - matching

		// NOTE: don't expect another searchScanFinished call, so not changing latch
		matches_latch.set( new CountDownLatch( 1 ) );
		if ( second_match_set != null ) {
			expected_matches.addAll( Arrays.asList( second_match_set ) );
		}
		System.out.println( "Expecting second: " + expected_matches );

		System.out.println( "Writing additional lines to test file..." );
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

	private void writeFile( String... lines ) {
		File tmp_file = null;
		PrintWriter tmp_writer = null;

		try {
			tmp_file = File.createTempFile( LogIndexerTest.class.getSimpleName() + "-", ".test.tmp" );
			tmp_writer = new PrintWriter( new FileWriter( tmp_file, false ) );

			for( String line : lines ) {
				tmp_writer.println( line );
			}
			tmp_writer.flush();
			IOKit.close( tmp_writer );
			tmp_writer = null;

			Files.move( tmp_file.toPath(), file.toPath(),
					StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			tmp_file = null;
		} catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
			if( tmp_file != null && tmp_file.isFile() ) {
				tmp_file.delete();
			}
			if( tmp_writer != null ) {
				IOKit.close( tmp_writer );
			}
		}
	}

	@ParameterizedTest
	@MethodSource( "argsLogIndexerTests" )
	public void testLogIndexer( TestArgs test_args )
		throws IOException, InterruptedException {

		int sleep_ms = 2000;
		int timeout_s = 20;

		if( test_args.getWriteAsAppend() ) {
			writer = new PrintWriter( new FileWriter( file, false ) );
		}

		final AtomicBoolean has_failure = new AtomicBoolean( false );

		final AtomicBoolean expected_full = new AtomicBoolean(true);

		final AtomicReference<CountDownLatch> indexing_finished_latch = new AtomicReference<>(
				new CountDownLatch( 1 ) );
		final AtomicInteger expected_total_rows = new AtomicInteger(0);

		final AtomicReference<CountDownLatch> matches_latch = new AtomicReference<>(
				new CountDownLatch( 0 ) );
		final AtomicReference<CountDownLatch> search_scan_finished_latch = new AtomicReference<>(
				new CountDownLatch( 0 ) );
		final List<SearchMatch> expected_matches = new ArrayList<>( 10 );

		final AtomicBoolean should_exceed_max_matches = new AtomicBoolean( false );

		LogIndexListener<File> log_index_listener = new LogIndexListener<>() {
			@Override
			public void indexingStarting( File file, boolean full ) {
				if( full != expected_full.get() ) {
					System.err.println( "Unexpected value for expected_full: " +
							full + " (expected " + expected_full.get() +
							")" );
					has_failure.set( true );
				}
			}

			@Override
			public void indexingFinished( File file, int total_rows ) {
				if( total_rows != expected_total_rows.get() ) {
					System.err.println( "Unexpected value for expected_total_rows: " +
							total_rows + " (expected " + expected_total_rows.get() +
							")" );
					has_failure.set( true );
				}

				indexing_finished_latch.get().countDown();
			}
		};

		indexer = new LogIndexer<>( file, file, log_index_listener, 2000, test_args.getMaxSearchHits(),
				test_args.getFullIndexAfterModified(), null );

		SearchListener listener = new SearchListener() {
			@Override
			public void searchScanFinished( int search_id, boolean exceed_max_matches ) {
				if ( expected_matches.isEmpty() ) {
					if ( should_exceed_max_matches.get() != exceed_max_matches ) {
						System.err.println( "Unexpected value for exceed_max_matches: " +
								exceed_max_matches + " (expected " + should_exceed_max_matches.get() +
								")" );
						has_failure.set( true );
					}

					search_scan_finished_latch.get().countDown();
				}
				else {
					System.out.println( "Told that search was finished when some " +
							"matches have not occurred. A search may start again though. " +
							"Not currently matched: " + expected_matches );
				}
			}

			@Override
			public void searchTermMatches( int search_id, SearchMatch... matches ) {
				if ( expected_matches.isEmpty() ) {
					System.err.println( "Unexpected call to searchTermMatches: " +
							Arrays.toString( matches ) );
					has_failure.set( true );
					return;
				}

				for( SearchMatch match : matches ) {
					if ( !expected_matches.remove( match ) ) {
						System.err.println( "Unexpected match in searchTermMatches: " +
								match + "  All returned matches: " +
								Arrays.toString( matches ) + "  Still expecting: " +
								expected_matches );
						has_failure.set( true );
					}
				}

				if ( expected_matches.isEmpty() ) {
					matches_latch.get().countDown();
				}
			}
		};

		// Start the initial search which returns nothing on the empty file
		indexer.startSearch( test_args.getParams(), listener );

		ThreadKit.sleep( sleep_ms );

		assertTrue( indexing_finished_latch.get().await( timeout_s, TimeUnit.SECONDS ),
				 test_args.getDescription() + " indexing_finished_latch.get().await " );

		for( TestUpdateArgs update_args : test_args.getUpdateArgs() ) {
			should_exceed_max_matches.set( update_args.getShouldExceedMaxMatches() );
			expected_matches.addAll( update_args.getExpectedMatches() );

			boolean expecting_matches = !expected_matches.isEmpty();

			expected_full.set( update_args.getExpectedIndexingFull() );
			expected_total_rows.set( update_args.getExpectedIndexingTotalRows() );

			indexing_finished_latch.set( new CountDownLatch( 1 ) );

			if( expecting_matches ) {
				matches_latch.set( new CountDownLatch( 1 ) );
				search_scan_finished_latch.set( new CountDownLatch( 1 ) );
			}

			if( test_args.getWriteAsAppend() ) {
				writeLines( update_args.getToWrite().toArray( new String[0] ) );
			} else {
				writeFile( update_args.getToWrite().toArray( new String[0] ) );
			}

			assertTrue( indexing_finished_latch.get().await( timeout_s, TimeUnit.SECONDS ),
					test_args.getDescription() + " indexing_finished_latch.get().await " );

			if( expecting_matches ) {
				assertTrue( matches_latch.get().await( timeout_s, TimeUnit.SECONDS ),
						test_args.getDescription() + " matches_latch.get().await " );
			}
			assertTrue( search_scan_finished_latch.get().await( timeout_s, TimeUnit.SECONDS ),
					test_args.getDescription() + " search_scan_finished_latch.get().await " );

			ThreadKit.sleep( sleep_ms );
			assertFalse( has_failure.get(), test_args.getDescription() + " has_failure.get() ");
			assertTrue( expected_matches.isEmpty(), test_args.getDescription() + " expected_matches.isEmpty()" );
		}
	}

	private static Stream<TestArgs> argsLogIndexerTests() {
		Stream<TestArgs> stream = Stream.empty();

		// =============================================================================================================
		// Test Case 1:
		// These duplicate the existing tests that use doTestSearching.
		// These are duplicated in order to add confidence for this alternative testing implementation
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
					"testSearching_SimpleSensitive",
					false,
					1000,
					SearchParams.createSimple( "hat", true ),
					true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch(1, 9, 3),
												new SearchMatch(1, 20, 3),
												new SearchMatch(1, 35, 3),
												new SearchMatch(4, 0, 3)
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										false,
										6,
										List.of(),
										false
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										false,
										7,
										List.of(
												new SearchMatch(6, 12, 3)
										),
										false
								)
						)
				)
			)
		);

		stream = Stream.concat( stream,
			Stream.of(
				new TestArgs(
					"testSearching_SimpleInsensitive",
					false,
					1000,
					SearchParams.createSimple( "hat", false ),
						true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 3, 0, 3 ),
												new SearchMatch( 4, 0, 3 )
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										false,
										6,
										List.of(),
										false
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										false,
										7,
										List.of(
												new SearchMatch( 6, 12, 3 )
										),
										false
								)
						)
				)
			)
		);

		stream = Stream.concat( stream,
			Stream.of(
				new TestArgs(
					"testSearching_Regex1",
					false,
					1000,
					SearchParams.createRegex( Pattern.compile( "hat" ) ),
						true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 4, 0, 3 )
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										false,
										6,
										List.of(),
										false
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										false,
										7,
										List.of(
												new SearchMatch( 6, 12, 3 )
										),
										false
								)
						)
				)
			)
		);

		stream = Stream.concat( stream,
			Stream.of(
				new TestArgs(
					"testSearching_maxHits",
					false,
					2,
					SearchParams.createSimple( "hat", false ),
						true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 )
										),
										true
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										false,
										6,
										List.of(),
										false
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										false,
										7,
										List.of(),
										false
								)
						)
				)
			)
		);

		// =============================================================================================================
		// Test Case 2:
		// Appending the same lines in the same number of steps as the above tests, but now with
		// full_index_after_modified == true. So we should expect a full index after every update, and the full search
		// results should be sent after every full index
		// =============================================================================================================
		stream = Stream.concat( stream,
			Stream.of(
				new TestArgs(
					"testSearching_SimpleSensitive_full",
					true,
					1000,
					SearchParams.createSimple( "hat", true ),
						true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch(1, 9, 3),
												new SearchMatch(1, 20, 3),
												new SearchMatch(1, 35, 3),
												new SearchMatch(4, 0, 3)
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										true,
										6,
										List.of(
												new SearchMatch(1, 9, 3),
												new SearchMatch(1, 20, 3),
												new SearchMatch(1, 35, 3),
												new SearchMatch(4, 0, 3)
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										true,
										7,
										List.of(
												new SearchMatch(1, 9, 3),
												new SearchMatch(1, 20, 3),
												new SearchMatch(1, 35, 3),
												new SearchMatch(4, 0, 3),
												new SearchMatch(6, 12, 3)
										),
										false
								)
						)
				)
			)
		);

		stream = Stream.concat( stream,
			Stream.of(
				new TestArgs(
					"testSearching_SimpleInsensitive_full",
					true,
					1000,
					SearchParams.createSimple( "hat", false ),
						true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 3, 0, 3 ),
												new SearchMatch( 4, 0, 3 )
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										true,
										6,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 3, 0, 3 ),
												new SearchMatch( 4, 0, 3 )
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										true,
										7,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 3, 0, 3 ),
												new SearchMatch( 4, 0, 3 ),
												new SearchMatch( 6, 12, 3 )
										),
										false
								)
						)
				)
			)
		);

		stream = Stream.concat( stream,
			Stream.of(
				new TestArgs(
					"testSearching_Regex1_full",
					true,
					1000,
					SearchParams.createRegex( Pattern.compile( "hat" ) ),
						true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 4, 0, 3 )
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										true,
										6,
										List.of(
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 4, 0, 3 )
										),
										false
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										true,
										7,
										List.of(
												new SearchMatch( 1, 9, 3 ),
												new SearchMatch( 1, 20, 3 ),
												new SearchMatch( 1, 35, 3 ),
												new SearchMatch( 4, 0, 3 ),
												new SearchMatch( 6, 12, 3 )
										),
										false
								)
						)
				)
			)
		);

		stream = Stream.concat( stream,
			Stream.of(
				new TestArgs(
					"testSearching_maxHits_full",
					true,
					2,
					SearchParams.createSimple( "hat", false ),
						true,
						List.of(
								new TestUpdateArgs(
										List.of(
												"Some line with no matches",
												"Hate to chat about that display of hats.",
												"Again, nothing to match",
												"Hat",
												"hat"
										),
										true,
										5,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 )
										),
										true
								),
								new TestUpdateArgs(
										List.of(
												"This shouldn't match"
										),
										true,
										6,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 )
										),
										true
								),
								new TestUpdateArgs(
										List.of(
												"I can haz a hat"
										),
										true,
										7,
										List.of(
												new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 1, 9, 3 )
										),
										true
								)
						)
				)
			)
		);

		// =============================================================================================================
		// Test Case 3:
		// Write the whole file each step but write the same lines along with the appended lines
		// full_index_after_modified==false
		// This should output the same results as Test Case 1.
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_SimpleSensitive_file",
								false,
								1000,
								SearchParams.createSimple( "hat", true ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3),
														new SearchMatch(4, 0, 3)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												false,
												6,
												List.of(),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												false,
												7,
												List.of(
														new SearchMatch(6, 12, 3)
												),
												false
										)
								)
						)
				)
		);

		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_SimpleInsensitive_file",
								false,
								1000,
								SearchParams.createSimple( "hat", false ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 3, 0, 3 ),
														new SearchMatch( 4, 0, 3 )
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												false,
												6,
												List.of(),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												false,
												7,
												List.of(
														new SearchMatch( 6, 12, 3 )
												),
												false
										)
								)
						)
				)
		);

		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_Regex1_file",
								false,
								1000,
								SearchParams.createRegex( Pattern.compile( "hat" ) ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 4, 0, 3 )
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												false,
												6,
												List.of(),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												false,
												7,
												List.of(
														new SearchMatch( 6, 12, 3 )
												),
												false
										)
								)
						)
				)
		);

		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_maxHits_file",
								false,
								2,
								SearchParams.createSimple( "hat", false ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 )
												),
												true
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												false,
												6,
												List.of(),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												false,
												7,
												List.of(),
												false
										)
								)
						)
				)
		);

		// =============================================================================================================
		// Test Case 4:
		// Write the whole file each step but write the same lines along with the appended lines
		// full_index_after_modified==true
		// This should output the same results as Test Case 2.
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_SimpleSensitive_full_file",
								true,
								1000,
								SearchParams.createSimple( "hat", true ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3),
														new SearchMatch(4, 0, 3)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												true,
												6,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3),
														new SearchMatch(4, 0, 3)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												true,
												7,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3),
														new SearchMatch(4, 0, 3),
														new SearchMatch(6, 12, 3)
												),
												false
										)
								)
						)
				)
		);

		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_SimpleInsensitive_full_file",
								true,
								1000,
								SearchParams.createSimple( "hat", false ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 3, 0, 3 ),
														new SearchMatch( 4, 0, 3 )
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												true,
												6,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 3, 0, 3 ),
														new SearchMatch( 4, 0, 3 )
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												true,
												7,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 3, 0, 3 ),
														new SearchMatch( 4, 0, 3 ),
														new SearchMatch( 6, 12, 3 )
												),
												false
										)
								)
						)
				)
		);

		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_Regex1_full_file",
								true,
								1000,
								SearchParams.createRegex( Pattern.compile( "hat" ) ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 4, 0, 3 )
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												true,
												6,
												List.of(
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 4, 0, 3 )
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												true,
												7,
												List.of(
														new SearchMatch( 1, 9, 3 ),
														new SearchMatch( 1, 20, 3 ),
														new SearchMatch( 1, 35, 3 ),
														new SearchMatch( 4, 0, 3 ),
														new SearchMatch( 6, 12, 3 )
												),
												false
										)
								)
						)
				)
		);

		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"testSearching_maxHits_full_file",
								true,
								2,
								SearchParams.createSimple( "hat", false ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat"
												),
												true,
												5,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 )
												),
												true
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match"
												),
												true,
												6,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 )
												),
												true
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"hat",
														"This shouldn't match",
														"I can haz a hat"
												),
												true,
												7,
												List.of(
														new SearchMatch( 1, 0, 3 ),
														new SearchMatch( 1, 9, 3 )
												),
												true
										)
								)
						)
				)
		);

		// =============================================================================================================
		// Test Case 5:
		// A in-place edit is missed when full_index_after_modified==false and the file size increased as a result of
		// the update
		// TODO: This test can be removed if the behavior is updated later as it's only used to demonstrate one of the
		// 		 limitations of the partial indexing implementation.
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"missed_in_place_edit",
								false,
								1000,
								SearchParams.createSimple( "hat", true ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"nothing"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														// in-place edit that would be missed when updating
														// "Hat" to "Hat hat"
														"Hat hat",
														"nothing"
												),
												false,
												// The expected_indexing_total_rows increases to 6 because the partial
												// indexing starts from the previous file length. This was the end of
												// file before the update. After the update, the previous index is 4
												// characters before the end of the file, so the new line is read again.
												6,
												List.of(),
												false
										)
								)
						)
				)
		);

		// =============================================================================================================
		// Test Case 6:
		// A in-place edit is caught when full_index_after_modified==true and the file size increased as a result of
		// the update
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"not_missed_in_place_edit_grow",
								true,
								1000,
								SearchParams.createSimple( "hat", true ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"nothing"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														// in-place edit that is caught
														"Hat hat",
														"nothing"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3),
														new SearchMatch(3, 4, 3)
												),
												false
										)
								)
						)
				)
		);

		// =============================================================================================================
		// Test Case 7:
		// A in-place edit is caught when full_index_after_modified==true and the file size does not change as a result
		// of the update
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"not_missed_in_place_edit_same",
								true,
								1000,
								SearchParams.createSimple( "hat", true ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"nothing"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														// in-place edit that is caught
														"hat",
														"nothing"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3),
														new SearchMatch(3, 0, 3)
												),
												false
										)
								)
						)
				)
		);

		// =============================================================================================================
		// Test Case 8:
		// A in-place edit is caught when full_index_after_modified==true and the file size decreases as a result
		// of the update
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"not_missed_in_place_edit_same",
								true,
								1000,
								SearchParams.createSimple( "hat", true ),
								false,
								List.of(
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														"Again, nothing to match",
														"Hat",
														"nothing"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"Some line with no matches",
														"Hate to chat about that display of hats.",
														// in-place edit that is caught
														"Again, hat",
														"Hat",
														"nothing"
												),
												true,
												5,
												List.of(
														new SearchMatch(1, 9, 3),
														new SearchMatch(1, 20, 3),
														new SearchMatch(1, 35, 3),
														new SearchMatch(2, 7, 3)
												),
												false
										)
								)
						)
				)
		);

		// =============================================================================================================
		// Test Case 9:
		// Full index when updating past 50 lines
		// =============================================================================================================
		stream = Stream.concat( stream,
				Stream.of(
						new TestArgs(
								"over_50_lines",
								true,
								1000,
								SearchParams.createSimple( "1a", true ),
								true,
								List.of(
										new TestUpdateArgs(
												List.of(
														"1a",
														"2a",
														"3a",
														"4a",
														"5a",
														"6a",
														"7a",
														"8a",
														"9a",
														"10a"
												),
												true,
												10,
												List.of(
														new SearchMatch(0, 0, 2)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"1b",
														"2b",
														"3b",
														"4b",
														"5b",
														"6b",
														"7b",
														"8b",
														"9b",
														"10b",
														"1c",
														"2c",
														"3c",
														"4c",
														"5c",
														"6c",
														"7c",
														"8c",
														"9c",
														"10c",
														"1d",
														"2d",
														"3d",
														"4d",
														"5d",
														"6d",
														"7d",
														"8d",
														"9d",
														"10d"
												),
												true,
												40,
												List.of(
														new SearchMatch(0, 0, 2)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"1b",
														"2b",
														"3b",
														"4b",
														"5b",
														"6b",
														"7b",
														"8b",
														"9b",
														"10b",
														"1c",
														"2c",
														"3c",
														"4c",
														"5c",
														"6c",
														"7c",
														"8c",
														"9c",
														"10c",
														"1d",
														"2d",
														"3d",
														"4d",
														"5d",
														"6d",
														"7d",
														"8d",
														"9d",
														"10d"
												),
												true,
												70,
												List.of(
														new SearchMatch(0, 0, 2)
												),
												false
										),
										new TestUpdateArgs(
												List.of(
														"1b",
														"2b",
														"3b",
														"4b",
														"5b",
														"6b",
														"7b",
														"8b",
														"9b",
														"10b",
														"1c",
														"2c",
														"3c",
														"4c",
														"5c",
														"6c",
														"7c",
														"8c",
														"9c",
														"10c",
														"1d",
														"2d",
														"3d",
														"4d",
														"5d",
														"6d",
														"7d",
														"8d",
														"9d",
														"10d"
												),
												true,
												100,
												List.of(
														new SearchMatch(0, 0, 2)
												),
												false
										)
								)
						)
				)
		);

		return stream;
	}

	public static class TestArgs {
		private final String description;

		private final boolean full_index_after_modified;
		private final int max_search_hits;
		private final SearchParams params;


		private final boolean write_as_append;
		private final List<TestUpdateArgs> update_args;

		TestArgs(
			String description,
			boolean full_index_after_modified,
			int max_search_hits,
			SearchParams param,
			final boolean write_as_append,
			List<TestUpdateArgs> update_args
		) {
			this.description = description;
			this.full_index_after_modified = full_index_after_modified;
			this.max_search_hits = max_search_hits;
			this.params = param;
			this.write_as_append = write_as_append;
			this.update_args = update_args;
		}

		public @Nonnull String getDescription() { return description; }
		public boolean getFullIndexAfterModified() { return full_index_after_modified; }
		public int getMaxSearchHits() { return max_search_hits; }
		public @Nonnull SearchParams getParams() { return params; }
		public boolean getWriteAsAppend() { return write_as_append; }
		public @Nonnull List<TestUpdateArgs> getUpdateArgs() { return update_args; }
	}

	public static class TestUpdateArgs {
		private final List<String> to_write;
		private final boolean expected_indexing_full;
		private final int expected_indexing_total_rows;
		private final List<SearchMatch> expected_matches;
		private final boolean should_exceed_max_matches;

		TestUpdateArgs(
			List<String> to_write,
			boolean expected_indexing_full,
			final int expected_indexing_total_rows,
			List<SearchMatch> expected_matches,
			boolean should_exceed_max_matches
		) {
			this.to_write = to_write;
			this.expected_indexing_full = expected_indexing_full;
			this.expected_indexing_total_rows = expected_indexing_total_rows;
			this.expected_matches = expected_matches;
			this.should_exceed_max_matches = should_exceed_max_matches;

			// Expecting each update to update the file
			// but matches could be empty after the update
			assertFalse( to_write.isEmpty() );
		}

		public @Nonnull List<String> getToWrite() { return to_write; }
		public boolean getExpectedIndexingFull() { return expected_indexing_full; }
		public int getExpectedIndexingTotalRows() { return expected_indexing_total_rows; }
		public @Nonnull List<SearchMatch> getExpectedMatches() { return expected_matches; }
		public boolean getShouldExceedMaxMatches() { return should_exceed_max_matches; }
	}
}
