/*
* Copyright (c) 2013 Rob Eden.
* All Rights Reserved.
*/

package com.logicartisan.io.log;

import com.logicartisan.common.core.listeners.ListenerSupport;
import com.logicartisan.common.core.thread.ObjectSlot;
import com.logicartisan.common.core.thread.SharedThreadPool;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import javafx.application.Platform;
import javafx.beans.InvalidationListener;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
* A data model ({@link ObservableList} backed by
*/
@SuppressWarnings( "UnusedDeclaration" )
public class LogObservableList<A> extends AbstractList<String>
	implements ObservableList<String>, LogIndexListener<A>, Closeable {

	private static final Logger LOG =
		LoggerFactory.getLogger( LogObservableList.class );

	private static final int[] EMPTY_INT_ARRAY = new int[ 0 ];

	private static final int CHUNK_SIZE = 25;



	private final ThreadPoolExecutor query_thread_pool;

	private final ListenerSupport<ListChangeListener,?> change_listeners =
		ListenerSupport.forType( ListChangeListener.class ).build();
	private final ListenerSupport<InvalidationListener,?> invalidation_listeners =
		ListenerSupport.forType( InvalidationListener.class ).build();

	private final LogAccess<A> access;

	private final TIntObjectMap<Chunk> line_map =
		new TIntObjectHashMap<>();
	private final ReferenceQueue chunk_ref_queue =
		new ReferenceQueue();

	private final TIntSet active_query_set = new TIntHashSet();
	private final Lock active_query_set_lock = new ReentrantLock();

	private final IntegerProperty row_property =
		new SimpleIntegerProperty( this, "rows", 0 );


	/**
	 * This constructor should be used if the model HAS NOT already been added as a
	 * listener.
	 */
	public LogObservableList( final LogAccess<A> access ) {
		this.access = access;
		query_thread_pool = createThreadPool();

		// Add listener
		SharedThreadPool.INSTANCE.execute( () -> {
			int line_count = access.addListener( LogObservableList.this );
			processNewRowCount( line_count );
		} );
	}

	/**
	 * This constructor should be used if the model HAS already been added as a listener.
	 */
	public LogObservableList( final LogAccess<A> access,
		int line_count ) {

		query_thread_pool = createThreadPool();
		this.access = access;

		processNewRowCount( line_count );
	}

	private ThreadPoolExecutor createThreadPool() {
		return new ThreadPoolExecutor( 1, 1, Long.MAX_VALUE,
			TimeUnit.DAYS, new ArrayBlockingQueue<>( 5 ),
			// This is a modified version of DiscardOldestPolicy that removes the index
			// that's no longer being queried for when a task is thrown away.
			( r, e ) -> {
	            if (!e.isShutdown()) {
	                Runnable to_discard = e.getQueue().poll();
		            if ( to_discard != null ) {
			            DataFetcher fetcher = ( DataFetcher ) to_discard;
			            active_query_set_lock.lock();
			            try {
				            active_query_set.remove( fetcher.index );
			            }
			            finally {
				            active_query_set_lock.unlock();
			            }
		            }

	                e.execute(r);
	            }
			} );
	}


	public void close() {
		LOG.info( "close" );

		try {
			access.removeListener( this );
		}
		catch( Exception ex ) {
			// ignore
		}

		change_listeners.removeAllListeners();
		invalidation_listeners.removeAllListeners();

		query_thread_pool.shutdownNow();
	}

	@Override
	public String get( int index ) {
		assert Platform.isFxApplicationThread();

		// Clean up map to remove GC'ed chunks
		Chunk ref;
		while( ( ref = ( Chunk ) chunk_ref_queue.poll() ) != null ) {
			if ( LOG.isDebugEnabled() ) {
				LOG.debug( "Chunk {} GC'ed", ref.index );
			}
			line_map.remove( ref.index );
		}


		// Find index to query within the chunk size
		int distance_from_main_index = index % CHUNK_SIZE;
		int main_index;
		if ( distance_from_main_index == 0 ) main_index = index;
		else main_index = index - distance_from_main_index;

		SoftReference<String[]> chunk_ref = line_map.get( main_index );
		String[] chunk = null;
		if ( chunk_ref != null ) chunk = chunk_ref.get();

		String line = null;
		if ( chunk != null ) line = chunk[ distance_from_main_index ];

		if ( line == null ) {
			active_query_set_lock.lock();
			try {
				// If a load isn't already in progress, start it now
				if ( !active_query_set.contains( main_index ) ) {
					if ( LOG.isDebugEnabled() ) {
						LOG.debug( "Begin query for chunk {} (line index {})", main_index,
							index );
					}
					query_thread_pool.execute( new DataFetcher( main_index ) );
//					System.out.println( "Active queries: " + active_query_set.size() );
					active_query_set.add( main_index );
				}
				else if ( LOG.isTraceEnabled() ) {
					LOG.trace( "Request for row {} found no data. Chunk {} already " +
						"being fetched.", index, main_index );
				}
			}
			finally {
				active_query_set_lock.unlock();
			}

			return "Loading...";
//			return Resources.MESSAGE_LOADING;
		}
		else {
			if ( LOG.isTraceEnabled() ) {
				LOG.trace( "Found data for row {}: {}", index, line );
			}
			return line;
		}
	}

	@Override
	public void indexingFinished( final A attachment, final int total_rows ) {
		if ( LOG.isDebugEnabled() ) {
			LOG.debug( "indexingFinished({},{})", attachment, total_rows );
		}
		processNewRowCount( total_rows );
	}

	@Override
	public void indexingStarting( A attachment, boolean full ) {
		if ( LOG.isDebugEnabled() ) {
			LOG.debug( "indexingStarting({},{})", attachment, full );
		}
	}


	@Override
	public int size() {
		if ( !Platform.isFxApplicationThread() ) {
			final ObjectSlot<Integer> value_slot = new ObjectSlot<>();
			Platform.runLater( () -> value_slot.set( row_property.getValue() ) );
			try {
				return value_slot.waitForValue();
			}
			catch ( InterruptedException e ) {
				// Ack. Throw caution to the wind and try direct access
				return row_property.get();
			}
		}
		else return row_property.get();
	}


	@Override
	public void addListener( ListChangeListener<? super String> listener ) {
		change_listeners.add( listener );
	}

	@Override
	public void removeListener( ListChangeListener<? super String> listener ) {
		change_listeners.remove( listener );
	}

	@Override
	public void addListener( InvalidationListener listener ) {
		invalidation_listeners.add( listener );
	}

	@Override
	public void removeListener( InvalidationListener listener ) {
		invalidation_listeners.remove( listener );
	}


	/**
	 * A thread-safe method that will process a new row count.
	 */
	private void processNewRowCount( final int rows ) {
		if ( !Platform.isFxApplicationThread() ) {
			Platform.runLater( () -> processNewRowCount( rows ) );
			return;
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debug( "New row count: {}", rows );
		}

		int previous_rows = row_property.get();
		row_property.set( rows );

		// See if there was previously a partial cache
		int distance_from_main_index = ( previous_rows - 1 ) % CHUNK_SIZE;
		if ( distance_from_main_index != ( CHUNK_SIZE - 1 ) ) {
			int main_index;
			if ( distance_from_main_index == 0 ) main_index = ( previous_rows - 1 );
			else main_index = ( previous_rows - 1 ) - distance_from_main_index;

			line_map.remove( main_index );
		}


		if ( rows < previous_rows ) {
			// If the file got smaller, reset everything
			//noinspection unchecked
			change_listeners.dispatch().onChanged(
				new ResetChange( previous_rows, rows ) );
			if ( LOG.isDebugEnabled() ) {
				LOG.debug( "Fire ResetChange: {} -> {}", previous_rows, rows );
			}
		}
		else {
			//noinspection unchecked
			change_listeners.dispatch().onChanged(
				new RowAddChange( previous_rows, rows ) );
			if ( LOG.isDebugEnabled() ) {
				LOG.debug( "Fire RowAddChange: {} -> {}", previous_rows, rows );
			}
		}

	}


	private class RowAddChange extends ListChangeListener.Change<String> {
		private boolean has_next = true;

		private final int from;
		private final int to;

		RowAddChange( int from, int to ) {
			super( LogObservableList.this );

			this.from = from;
			this.to = to;

			assert from < to : from + " >= " + to;
		}

		@Override
		public boolean next() {
			if ( !has_next ) return false;

			has_next = false;
			return true;
		}

		@Override
		public void reset() {
			has_next = true;
		}

		@Override
		public int getFrom() {
			return from;
		}

		@Override
		public int getTo() {
			return to;
		}

		@Override
		public List<String> getRemoved() {
			return Collections.emptyList();
		}

		@Override
		protected int[] getPermutation() {
			return EMPTY_INT_ARRAY;
		}
	}


	private class ResetChange extends ListChangeListener.Change<String> {
		// 0 - next() not called
		// 1 - remove all items
		// 2 - add new items
		private int mode = 0;

		private final int size_before;
		private final int size_after;

		ResetChange( int size_before, int size_after ) {
			super( LogObservableList.this );

			this.size_before = size_before;
			this.size_after = size_after;

			assert size_after < size_before : size_after + " >= " + size_before;
		}


		@Override
		public boolean next() {
			mode++;
			return mode <= 2;
		}

		@Override
		public void reset() {
			mode = 0;
		}

		@Override
		public int getFrom() {
			return 0;
		}

		@Override
		public int getTo() {
			if ( mode == 1 ) return size_before;
			else return size_after;
		}

		@Override
		public boolean wasRemoved() {
			return mode == 1;
		}

		@Override
		public boolean wasAdded() {
			return mode == 2;
		}

		@Override
		public List<String> getRemoved() {
			if ( mode != -1 ) return Collections.emptyList();
			return new FakeList( size_before );
		}

		@Override
		public List<String> getAddedSubList() {
			if ( mode != 2 ) return Collections.emptyList();
			return getList().subList( 0, size_after );
		}

		@Override
		protected int[] getPermutation() {
			return EMPTY_INT_ARRAY;
		}
	}

	private class DataFetcher implements Runnable {
		private final int index;

		DataFetcher( int index ) {
			this.index = index;
		}

		@Override
		public void run() {
			Thread.currentThread().setName( "DataFetcher: " + index );
			try {
				String[] lines;
				try {
	//				System.out.println( "Fetch " + index );
					lines = access.readLines( index, CHUNK_SIZE );

					if ( lines[ 0 ] == null ) {
						if ( LOG.isInfoEnabled() ) {
							LOG.info( "Query for chunk {} gave all nulls",
								index );
						}
						return;
					}
				}
				catch( Throwable ex ) {
					if ( LOG.isWarnEnabled() ) {
						LOG.warn( "Error reading lines: {} (count: {})", index,
							CHUNK_SIZE, ex );
					}
					lines = new String[ CHUNK_SIZE ];
					String line = "Error reading: " +
						( ex.getMessage() == null ? ex.toString() : ex.getMessage() );
					for( int i = 0; i < lines.length; i++ ) {
						lines[ i ] = line;
					}
				}

				// Wait for the set to complete so we don't remove from the
				// active_query_set to quickly.
				CountDownLatch setter_latch = new CountDownLatch( 1 );
				Platform.runLater( new LineSetter( index, lines, setter_latch ) );
				try {
					setter_latch.await();
				}
				catch ( InterruptedException e ) {
					// ignore
				}
			}
			finally {
				active_query_set_lock.lock();
				try {
					active_query_set.remove( index );
				}
				finally {
					active_query_set_lock.unlock();
				}
			}
		}
	}


	private class LineSetter extends ListChangeListener.Change<String>
		implements Runnable {

		private final int index;
		private final String[] lines;

		private final CountDownLatch setter_latch;

		private boolean has_next = true;

		LineSetter( int index, String[] lines, CountDownLatch setter_latch ) {
			super( LogObservableList.this );

			this.index = index;
			this.lines = lines;
			this.setter_latch = setter_latch;
		}

		@Override
		public void run() {
			try {
				assert Platform.isFxApplicationThread();

				if ( LOG.isDebugEnabled() ) {
					LOG.debug( "Put data for chunk {}. Non-null lines: {}",
						index,
						findNonNullLines( lines ) );
				}
				line_map.put( index, new Chunk( lines, chunk_ref_queue, index ) );

				// Fire change notification
				//noinspection unchecked
				change_listeners.dispatch().onChanged( this );
			}
			finally {
				setter_latch.countDown();
			}
		}


		@Override
		public int getFrom() {
			return index;
		}

		@Override
		public boolean next() {
			if ( !has_next ) return false;

			has_next = false;
			return true;
		}

		@Override
		public void reset() {
			has_next = true;
		}

		@Override
		public int getTo() {
			return index + lines.length;
		}

		@Override
		public List<String> getRemoved() {
			return Collections.emptyList();
		}

		@Override
		protected int[] getPermutation() {
			return EMPTY_INT_ARRAY;
		}


		private int findNonNullLines( String[] lines ) {
			int count = 0;
			for( String line : lines ) {
				if ( line == null ) break;
				count++;
			}
			return count;
		}
	}


	private class Chunk extends SoftReference<String[]> {
		private final int index;

		Chunk( String[] chunk, ReferenceQueue<? super String[]> q,
			int index ) {

			super( chunk, q );
			this.index = index;
		}
	}


	private class FakeList extends AbstractList<String> {
		private final int size;

		FakeList( int size ) {
			this.size = size;
		}

		@Override
		public String get( int index ) {
			return "Loading...";
		}

		@Override
		public int size() {
			return size;
		}
	}


	///////////////////////////////////////////////////////
	// Unsupported methods


	@Override
	public boolean addAll( String... strings ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void remove( int i, int i2 ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll( String... strings ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll( String... strings ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean setAll( Collection<? extends String> strings ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean setAll( String... strings ) {
		throw new UnsupportedOperationException();
	}
}
