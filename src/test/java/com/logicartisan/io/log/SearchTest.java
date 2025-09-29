package com.logicartisan.io.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;


/**
 *
 */
public class SearchTest {
	private LogIndexer<File> indexer = null;


	@AfterEach
	public void tearDown() {
		if ( indexer != null ) {
			indexer.close();
			indexer = null;
		}
	}


	/**
	 * Regex single character search with multiple adjacent matches within the same line.
	 */
	@Test
	public void testSingleCharacterSearch() throws Exception {

		final List<SearchMatch> expected = new LinkedList<>();
		expected.add( new SearchMatch( 0, 1, 1 ) );
		expected.add( new SearchMatch( 0, 2, 1 ) );

		SearchParams params = SearchParams.createRegex( Pattern.compile("\\d") );

		testBundleLogFile( "adjacent_characters_test.log", params, expected );
	}


	/**
	 * Real world test case of search for "COMMAND" in "openlmi-install.log"
	 * (in resources). Original problem was a line mismatch.
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
		LogIndexListener<File> listener = new LogIndexListener<>() {
            @Override
            public void indexingStarting(File attachment, boolean full) {
            }

            @Override
            public void indexingFinished(File attachment, int total_rows) {
                initial_index_complete_latch.countDown();
            }
        };

		indexer = new LogIndexer<>( file, file,
				listener, 1000, 100, null );

		assertTrue(
			initial_index_complete_latch.await( 5, TimeUnit.SECONDS ),
			"Timed out waiting for initial index" );

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

		assertTrue( search_complete_latch.await( 5, TimeUnit.SECONDS ),
			 "Timed out waiting for search completion" );
		assertEquals( expected_match_count, matches_count.get() );

		assertNull( error_slot.get() );
		assertTrue( expected.isEmpty(), expected.toString() );



		// Read every line and make sure they're as expected
		int index = 0;
		try( BufferedReader in = new BufferedReader( new FileReader( file ) ) ) {
			String line;
			while( ( line = in.readLine() ) != null ) {
				String[] lines = indexer.readLines( index, 1 );
				assertEquals( line, lines[ 0 ], "Mismatch at index " + index );

				System.out.println( "Line " + index + ": " + line );
				index++;
			}
		}
		// NOTE: Adding one to the index because BufferedReader doesn't count the final
		//       line, which is simply a blank line
		assertEquals( indexer.getLineCount(), index );
	}


	private static Collection<ResumeSearchTestArgs> resumeSearchTestArgs() {

		Stream<ResumeSearchTestArgs> stream = Stream.empty();

		String multi_lines_file_name = "resume_test_lines.log";

		stream = Stream.concat( stream,
			Stream.of(
                    new ResumeSearchTestArgs(
                            1,
                            new SearchParams.SimpleSearchParams("ONE", false),
                            List.of( List.of( new SearchMatch( 0, 0, 3 ) ) ),
							multi_lines_file_name,
                            "Multi-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume not called."
                    )
		) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ONE" ) ),
								List.of( List.of( new SearchMatch( 0, 0, 3 ) ) ),
								multi_lines_file_name,
								"REGEX Multi-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume not called."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("ONE", false),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of()
								),
								multi_lines_file_name,
								"Multi-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume called once without results."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ONE" ) ),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of()
								),
								multi_lines_file_name,
								"REGEX Multi-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume called once without results."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("ONE", false),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of(),
										List.of(),
										List.of()
								),
								multi_lines_file_name,
								"Multi-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume called multiple times without results."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ONE" ) ),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of(),
										List.of(),
										List.of()
								),
								multi_lines_file_name,
								"REGEX Multi-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume called multiple times without results."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("TWO", false),
								List.of(
										List.of( new SearchMatch( 1, 0, 3 ) ),
										List.of( new SearchMatch( 11, 0, 3 ) )
								),
								multi_lines_file_name,
								"Multi-line: TWO - Initial search completes after finding 1 of 2 tokens. 2 of 2 tokens found after calling resume 1 times."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "TWO" ) ),
								List.of(
										List.of( new SearchMatch( 1, 0, 3 ) ),
										List.of( new SearchMatch( 11, 0, 3 ) )
								),
								multi_lines_file_name,
								"REGEX Multi-line: TWO - Initial search completes after finding 1 of 2 tokens. 2 of 2 tokens found after calling resume 1 times."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								new SearchParams.SimpleSearchParams("TWO", false),
								List.of(
										List.of( new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 11, 0, 3 ) )
								),
								multi_lines_file_name,
								"Multi-line: TWO - Initial search completes after finding 2 of 2 tokens. Resume not called."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								SearchParams.createRegex( Pattern.compile( "TWO" ) ),
								List.of(
										List.of( new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 11, 0, 3 ) )
								),
								multi_lines_file_name,
								"REGEX Multi-line: TWO - Initial search completes after finding 2 of 2 tokens. Resume not called."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								new SearchParams.SimpleSearchParams("TWO", false),
								List.of(
										List.of( new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 11, 0, 3 ) ),
										List.of()
								),
								multi_lines_file_name,
								"Multi-line: TWO - Initial search completes after finding 2 of 2 tokens. Resume called once without results."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								SearchParams.createRegex( Pattern.compile( "TWO" ) ),
								List.of(
										List.of( new SearchMatch( 1, 0, 3 ),
												new SearchMatch( 11, 0, 3 ) ),
										List.of()
								),
								multi_lines_file_name,
								"REGEX Multi-line: TWO - Initial search completes after finding 2 of 2 tokens. Resume called once without results."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("ZERO", false),
								List.of(
										List.of( new SearchMatch( 65, 0, 4 ) ),
										List.of( new SearchMatch( 66, 0, 4 ) ),
										List.of( new SearchMatch( 67, 0, 4 ) ),
										List.of( new SearchMatch( 68, 0, 4 ) ),
										List.of( new SearchMatch( 69, 0, 4 ) )
								),
								multi_lines_file_name,
								"Multi-line: ZERO - Initial search completes after finding 1 of 5 tokens. 5 of 5 tokens found after calling resume 4 times."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ZERO" ) ),
								List.of(
										List.of( new SearchMatch( 65, 0, 4 ) ),
										List.of( new SearchMatch( 66, 0, 4 ) ),
										List.of( new SearchMatch( 67, 0, 4 ) ),
										List.of( new SearchMatch( 68, 0, 4 ) ),
										List.of( new SearchMatch( 69, 0, 4 ) )
								),
								multi_lines_file_name,
								"REGEX Multi-line: ZERO - Initial search completes after finding 1 of 5 tokens. 5 of 5 tokens found after calling resume 4 times."
						)
				) );



		String single_line_file_name = "resume_test_line.log";
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("ONE", false),
								List.of( List.of( new SearchMatch( 0, 0, 3 ) ) ),
								single_line_file_name,
								"Single-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume not called."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ONE" ) ),
								List.of( List.of( new SearchMatch( 0, 0, 3 ) ) ),
								single_line_file_name,
								"REGEX Single-line: ONE - Initial search completes after finding 1 of 1 tokens. Resume not called."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("ONE", false),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of()
								),
								single_line_file_name,
								"Single-line: ONE - Initial search completes after finding 1 of 1. Resume called once without results."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ONE" ) ),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of()
								),
								single_line_file_name,
								"REGEX Single-line: ONE - Initial search completes after finding 1 of 1. Resume called once without results."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("ONE", false),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of(),
										List.of(),
										List.of()
								),
								single_line_file_name,
								"Single-line: ONE - Initial search completes after finding 1 of 1. Resume called multiple times without results."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ONE" ) ),
								List.of(
										List.of( new SearchMatch( 0, 0, 3 ) ),
										List.of(),
										List.of(),
										List.of()
								),
								single_line_file_name,
								"REGEX Single-line: ONE - Initial search completes after finding 1 of 1. Resume called multiple times without results."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("TWO", false),
								List.of(
										List.of( new SearchMatch( 0, 3, 3 ) ),
										List.of( new SearchMatch( 0, 39, 3 ) )
								),
								single_line_file_name,
								"Single-line: TWO - Initial search completes after finding 1 of 2 tokens. 2 of 2 tokens found after calling resume 1 times."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "TWO" ) ),
								List.of(
										List.of( new SearchMatch( 0, 3, 3 ) ),
										List.of( new SearchMatch( 0, 39, 3 ) )
								),
								single_line_file_name,
								"REGEX Single-line: TWO - Initial search completes after finding 1 of 2 tokens. 2 of 2 tokens found after calling resume 1 times."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								new SearchParams.SimpleSearchParams("TWO", false),
								List.of(
										List.of(
												new SearchMatch( 0, 3, 3 ),
												new SearchMatch( 0, 39, 3 )
										)
								),
								single_line_file_name,
								"Single-line: TWO - Initial search completes after finding 2 of 2 tokens. Resume not called."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								SearchParams.createRegex( Pattern.compile( "TWO" ) ),
								List.of(
										List.of(
												new SearchMatch( 0, 3, 3 ),
												new SearchMatch( 0, 39, 3 )
										)
								),
								single_line_file_name,
								"REGEX Single-line: TWO - Initial search completes after finding 2 of 2 tokens. Resume not called."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								new SearchParams.SimpleSearchParams("ZERO", false),
								List.of(
										List.of( new SearchMatch( 0, 219, 4 ) ),
										List.of( new SearchMatch( 0, 223, 4 ) ),
										List.of( new SearchMatch( 0, 227, 4 ) ),
										List.of( new SearchMatch( 0, 231, 4 ) ),
										List.of( new SearchMatch( 0, 235, 4 ) )
								),
								single_line_file_name,
								"Single-line: ZERO - Initial search completes after finding 1 of 5 tokens. 5 of 5 tokens found after calling resume 4 times."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								1,
								SearchParams.createRegex( Pattern.compile( "ZERO" ) ),
								List.of(
										List.of( new SearchMatch( 0, 219, 4 ) ),
										List.of( new SearchMatch( 0, 223, 4 ) ),
										List.of( new SearchMatch( 0, 227, 4 ) ),
										List.of( new SearchMatch( 0, 231, 4 ) ),
										List.of( new SearchMatch( 0, 235, 4 ) )
								),
								single_line_file_name,
								"REGEX Single-line: ZERO - Initial search completes after finding 1 of 5 tokens. 5 of 5 tokens found after calling resume 4 times."
						)
				) );

		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								new SearchParams.SimpleSearchParams("ZERO", false),
								List.of(
										List.of(
												new SearchMatch( 0, 219, 4 ),
												new SearchMatch( 0, 223, 4 )
										),
										List.of(
												new SearchMatch( 0, 227, 4 ),
												new SearchMatch( 0, 231, 4 )
										),
										List.of( new SearchMatch( 0, 235, 4 ) )
								),
								single_line_file_name,
								"Single-line: ZERO - Initial search completes after finding 2 of 5 tokens. 5 of 5 tokens found after calling resume 2 times."
						)
				) );
		stream = Stream.concat( stream,
				Stream.of(
						new ResumeSearchTestArgs(
								2,
								SearchParams.createRegex( Pattern.compile( "ZERO" ) ),
								List.of(
										List.of(
												new SearchMatch( 0, 219, 4 ),
												new SearchMatch( 0, 223, 4 )
										),
										List.of(
												new SearchMatch( 0, 227, 4 ),
												new SearchMatch( 0, 231, 4 )
										),
										List.of( new SearchMatch( 0, 235, 4 ) )
								),
								single_line_file_name,
								"REGEX Single-line: ZERO - Initial search completes after finding 2 of 5 tokens. 5 of 5 tokens found after calling resume 2 times."
						)
				) );


		return stream.collect( Collectors.toSet() );
	}


	@ParameterizedTest
	@MethodSource( "resumeSearchTestArgs" )
	public void testResumeSearch( ResumeSearchTestArgs args ) throws Exception {

		File file =
				Paths.get( SearchTest.class.getResource( args.getFileName() ).toURI() ).toFile();

		final CountDownLatch initial_index_complete_latch = new CountDownLatch( 1 );
		LogIndexListener<File> listener = new LogIndexListener<>() {
			@Override
			public void indexingStarting(File attachment, boolean full) {
			}

			@Override
			public void indexingFinished(File attachment, int total_rows) {
				initial_index_complete_latch.countDown();
			}
		};

		indexer = new LogIndexer<>( file, file,
				listener, 1000, args.getMaxSearchHits(), null );

		assertTrue(
				initial_index_complete_latch.await( 5, TimeUnit.SECONDS ),
				"Timed out waiting for initial index" );

		final CopyOnWriteArrayList<SearchMatch> search_matches = new CopyOnWriteArrayList<>();
		final Semaphore search_complete_semaphore = new Semaphore( 1 );

		SearchListener search_listener = new SearchListener() {
			@Override
			public void searchScanFinished( int search_id, boolean exceed_max_matches ) {
				System.out.println(
						"searchScanFinished search_id: " + search_id + ", exceed_max_matches: " + exceed_max_matches );
				search_complete_semaphore.release();
			}

			@Override
			public void searchTermMatches( int search_id, SearchMatch... matches ) {
				System.out.println(
						"searchTermMatches search_id: " + search_id + ", matches: " + Arrays.toString( matches ) );
				search_matches.addAll( List.of( matches ) );
			}
		};

		Integer search_id = null;
		for( List<SearchMatch> expected : args.getExpected() ) {
			search_complete_semaphore.acquire();
			search_matches.clear();

			if( search_id == null ) {
				search_id = Integer.valueOf(
						indexer.startSearch( args.getSearchParams(), search_listener ) );
			} else {
				if( !indexer.resumeSearch( search_id.intValue() ) ) {
					search_complete_semaphore.release();
				}
			}

			assertTrue( search_complete_semaphore.tryAcquire( 5, TimeUnit.SECONDS ),
					"Timed out waiting for search completion during test: " + args.getTestCase() );
			search_complete_semaphore.release();

            List<SearchMatch> actual_matches = new ArrayList<>(search_matches);

			assertEquals( expected, actual_matches, args.getTestCase() );
		}

		// Read every line and make sure they're as expected
		int index = 0;
		try( BufferedReader in = new BufferedReader( new FileReader( file ) ) ) {
			String line;
			while( ( line = in.readLine() ) != null ) {
				String[] lines = indexer.readLines( index, 1 );
				assertEquals( line, lines[ 0 ],
						"Mismatch at index " + index + " during test: \"" + args.getTestCase() + "\"" );
				index++;
			}
		}
		// NOTE: Adding one to the index because BufferedReader doesn't count the final
		//       line, which is simply a blank line
		assertEquals( indexer.getLineCount(), index );
	}

	public static class ResumeSearchTestArgs {

		private final int max_search_hits;
		private final SearchParams search_params;
		private final List<List<SearchMatch>> expected;
		private final String file_name;
		private final String test_case;

		public ResumeSearchTestArgs(
				int max_search_hits,
				SearchParams search_params,
				List<List<SearchMatch>> expected,
				String file_name,
				String test_case
		) {
			this.max_search_hits = max_search_hits;
			this.search_params = search_params;
			this.expected = expected;
			this.file_name = file_name;
			this.test_case = test_case;
		}

		public int getMaxSearchHits() { return max_search_hits; }

		public SearchParams getSearchParams() { return search_params; }

		public List<List<SearchMatch>> getExpected() { return expected; }

		public String getFileName() { return file_name; }

		public String getTestCase() { return test_case; }
	}

}
