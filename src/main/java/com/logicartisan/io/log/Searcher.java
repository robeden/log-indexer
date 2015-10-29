package com.logicartisan.io.log;

import com.starlight.listeners.ListenerSupport;
import com.starlight.listeners.MessageDeliveryErrorHandler;
import com.starlight.thread.SharedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 *
 */
class Searcher<A>
	implements Runnable, MessageDeliveryErrorHandler<SearchListener>, LogIndexListener<A> {

	private static final Logger LOG = LoggerFactory.getLogger( Searcher.class );

	private static final int MATCH_NOTIFY_BATCH_SIZE = 50;

	private static final int MAX_ERRORS_TO_CANCEL = 3;

	private final LogIndexer parent;
	private final int id;
	private final int max_search_hits;

	private final Matcher matcher;
	private final String token;
	private final boolean case_sensitive;

	private final ListenerSupport<SearchListener,?> listeners;

	private final AtomicBoolean running = new AtomicBoolean( false );
	private final AtomicInteger processed_lines = new AtomicInteger( 0 );

	private final Lock state_lock = new ReentrantLock();
	private volatile int state_known_lines = 0;
	private volatile boolean state_index_reset = false;

	private volatile boolean keep_going = true;

	private final AtomicInteger hit_counter = new AtomicInteger( 0 );


	Searcher( LogIndexer parent, int id, SearchParams params,
		SearchListener listener, int max_search_hits ) {

//		System.out.println( "Listener for searcher: " + listener );

		this.parent = parent;
		this.id = id;
		this.max_search_hits = max_search_hits;

		if ( params instanceof SearchParams.RegexSearchParams ) {
			Pattern pattern = ( ( SearchParams.RegexSearchParams ) params ).getPattern();
			matcher = pattern.matcher( "" );
			token = null;
			case_sensitive = false;
		}
		else if ( params instanceof SearchParams.SimpleSearchParams ) {
			SearchParams.SimpleSearchParams sparams =
				( SearchParams.SimpleSearchParams ) params;
			matcher = null;
			case_sensitive = sparams.isCaseSensitive();
			// NOTE: if not case sensitive, store token lower case
			token = case_sensitive ? sparams.getToken() : sparams.getToken().toLowerCase();
		}
		else throw new UnsupportedOperationException( "Unknown param type: " +
			params.getClass().getName() );

		// NOTE: this will only ever have one listener, but simplifies dispatching and
		//       preserving message order.
		listeners = ListenerSupport.forType( SearchListener.class )
			.executor( SharedThreadPool.INSTANCE )
			.errorHandler( this )
			.build();
		listeners.add( listener );

		state_lock.lock();
		try {
			// Listen for changes from the parent
			state_known_lines = parent.addListener( this );

			// If there is data, start processing
			if ( state_known_lines != 0 && running.compareAndSet( false, true ) ) {
				SharedThreadPool.INSTANCE.execute( this );
			}
		}
		finally {
			state_lock.unlock();
		}
	}


	/**
	 * Stop searching.
	 */
	void halt() {
		keep_going = false;
	}


	///////////////////////////////////////////////
	// From Runnable

	@Override
	public void run() {
		Thread current_thread = Thread.currentThread();
		current_thread.setName( "LogIndexer-Searcher-" + id );
		current_thread.setPriority( Thread.MIN_PRIORITY + 2 );

		try {
			inner_run();
		}
		finally {
//			LOG.debug( "Scan finished: " + n)

			listeners.dispatch().searchScanFinished( id,
				hit_counter.get() >= max_search_hits );

			// Indicate we're no longer running
			running.set( false );

			// Make sure we didn't miss changes and need to run again
			checkStartThread();
		}
	}

	private boolean inner_run() {
		while( keep_going ) {

			boolean reset;
			int known_lines;
			state_lock.lock();
			try {
				reset = state_index_reset;
				known_lines = state_known_lines;
			}
			finally {
				state_lock.unlock();
			}

			// First, check to see if we're been reset
			if ( reset ) {
				processed_lines.set( 0 );
			}

			if ( processed_lines.get() >= known_lines ) return false;   // done

			if ( LOG.isDebugEnabled() ) {
				LOG.debug( "Search => processed_lines: {}  known_lines: {}  reset: {}  " +
					"hit_counter: {}", processed_lines, known_lines, reset, hit_counter );
			}

			final Queue<SearchMatch> match_notification_queue = new LinkedList<>();

			final AtomicBoolean hit_max = new AtomicBoolean( false );
			try {
				int streamed_lines = parent.streamLines( processed_lines.get(),
					( line, line_index ) -> {
						// NOTE: according to LogIndexer API, unavailable lines
						//       will have a null value.
						if ( line == null ) {
							LOG.debug( "Null content returned for line: {}", line_index );
							return processed_lines.incrementAndGet() < known_lines;
						}

						try {
							if ( searchLine( ( String ) line, line_index,
								match_notification_queue, hit_counter ) ) {

								sendNotifications( match_notification_queue, false );
							}
						}
						catch( SearchLimitReachedException ex ) {
							hit_max.set( true );
							keep_going = false;
							return false;
						}

						// It's possible we could read more lines than we were notified about
						// at the top of the loop due to timing. So, stop if we hit what we
						// were aware of at the top of the loop to avoid odd scenarios.
						if ( processed_lines.incrementAndGet() >= known_lines ) {
							return false;
						}

						return keep_going;
					} );
				LOG.debug( "Streamed lines: {}", streamed_lines );
			}
			catch ( IOException e ) {
				LOG.warn( "Error searching log file: {}", parent.getAttachment(), e );
				return false;     // retry when a change is made
			}
			finally {
				sendNotifications( match_notification_queue, true );    // flush
			}

			if ( hit_max.get() ) return true;
		}

		return false;
	}

	/**
	 * @return      True if the line matched
	 */
	private boolean searchLine( String line, int row_index,
		Queue<SearchMatch> match_notification_queue, AtomicInteger hit_counter )
		throws SearchLimitReachedException {

		if ( line.length() == 0 ) return false;

		boolean hit = false;
		if ( matcher != null ) {
			matcher.reset( line );

			int start_position = 0;
			while( matcher.find( start_position ) ) {
				hit = true;

				int start = matcher.start();
				SearchMatch match =
					new SearchMatch( row_index, start, matcher.end() - start );
				match_notification_queue.add( match );

				// See if we've exceeded the max number of hits
				int hit_count = hit_counter.incrementAndGet();
				if ( hit_count >= max_search_hits ) {
					throw new SearchLimitReachedException();
				}

				start_position = matcher.end() + 1;
				if ( start_position >= line.length() ) break;
			}
		}
		else {
			// Make line lower case (matching how the token is stored) is matching case
			// in-sensitively.
			String search_line = case_sensitive ? line : line.toLowerCase();

			int start_search_index = 0;
			int match_index;
			while( ( match_index =
				search_line.indexOf( token, start_search_index ) ) >= 0 ) {

				hit = true;

				SearchMatch match =
					new SearchMatch( row_index, match_index, token.length() );
				match_notification_queue.add( match );

				// See if we've exceeded the max number of hits
				int hit_count = hit_counter.incrementAndGet();
				if ( hit_count >= max_search_hits ) {
					throw new SearchLimitReachedException();
				}

				start_search_index = match_index + token.length();
			}
		}

		return hit;
	}

	private void sendNotifications( Queue<SearchMatch> match_notification_queue,
		boolean flush ) {

		int queue_size = match_notification_queue.size();

		// If we're not sending everything (flushing) and we have less than the batch
		// size, then there's nothing to do right now.
		if ( !flush && queue_size < MATCH_NOTIFY_BATCH_SIZE ) return;

		// Keep going while either:
		//  1) We're flushing and there's still data
		//  2) We're not flushing and there's still a batch or more data left
		while( ( flush && queue_size != 0 ) ||
			( !flush && queue_size >= MATCH_NOTIFY_BATCH_SIZE ) ) {

			SearchMatch[] matches =
				new SearchMatch[ Math.min( queue_size, MATCH_NOTIFY_BATCH_SIZE ) ];
			for( int i = 0; i < matches.length; i++ ) {
				matches[ i ] = match_notification_queue.poll();
				assert matches[ i ] != null :
					"Null return from poll? " + match_notification_queue +
					" (flush=" + flush + ", queue_size=" + queue_size +
					", matches.length=" + matches.length + ", i=" + i +", queue=" +
					match_notification_queue + ")";
			}

			// Fire to listeners
			listeners.dispatch().searchTermMatches( id, matches );

			queue_size = match_notification_queue.size();
		}

		assert !flush || match_notification_queue.isEmpty() :
			"Queue=" + match_notification_queue;
	}

	///////////////////////////////////////////////
	// From LogIndexListener

	@Override
	public void indexingStarting( A attachment, boolean full ) {
		// If a full re-index is happening, reset the known lines and then indicate the
		// reset.
		if ( full ) {
			state_lock.lock();
			try {
				state_known_lines = 0;
				state_index_reset = true;
			}
			finally {
				state_lock.unlock();
			}
		}
	}

	@Override
	public void indexingFinished( A attachment, int total_rows ) {
		LOG.debug( "Notified of index completion: {} - {}", attachment, total_rows );

		state_lock.lock();
		try {
			// Nothing to do, dup notification
			if ( total_rows == state_known_lines ) return;

			state_known_lines = total_rows;
		}
		finally {
			state_lock.unlock();
		}

		if ( hit_counter.get() >= max_search_hits ) return;

		checkStartThread();
	}


	private void checkStartThread() {
		if ( !keep_going ) return;

		state_lock.lock();
		try {
			if ( processed_lines.get() >= state_known_lines ) return;
		}
		finally {
			state_lock.unlock();
		}
		if ( hit_counter.get() >= max_search_hits ) return;

		// If the thread isn't processing, start it
		if ( running.compareAndSet( false, true ) ) {
			LOG.debug( "Searcher thread is not running. Will start" );
			SharedThreadPool.INSTANCE.execute( this );
		}
		else {
			LOG.debug( "Searcher thread is already running." );
		}
	}


	///////////////////////////////////////////////
	// From SearchListener

	@Override
	public ErrorResponse deliveryError(
		SearchListener listener, Throwable throwable, int overall_error_count,
		int overall_success_count, int consecutive_errors, boolean fatal ) {

		if ( throwable instanceof AssertionError ) {
			throwable.printStackTrace();
			return ErrorResponse.DROP_MESSAGE;
		}

		// If it's a fatal
		if ( fatal || consecutive_errors >= MAX_ERRORS_TO_CANCEL ) {
			halt();
			return ErrorResponse.REMOVE_LISTENER;
		}

		return ErrorResponse.RETRY_MESSAGE;
	}

	@Override
	public ErrorResponse excessiveBacklog(
		SearchListener listener, int backlog_size, int overall_error_count,
		int overall_success_count, int consecutive_errors ) {

		return ErrorResponse.REMOVE_LISTENER;
	}

	@Override
	public void lastListenerRemoved() {}
}
