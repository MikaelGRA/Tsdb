using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public class TsdbWriteBatcher<TKey, TEntry> : IDisposable
      where TEntry : IEntry
   {
      private static readonly TimeSpan MinWarningInterval = TimeSpan.FromMinutes( 1 );

      private object _sync = new object();
      private TsdbClient<TKey, TEntry> _client;
      private TimeSpan _writeInterval;
      private PublicationType _publishType;
      private Publish _publishMode;
      private bool _useTempStorage;
      private bool _disposed = false;
      private CancellationTokenSource _cts;
      private ITsdbLogger _logger;
      private int _maxBatchSize;
      private DateTime? _lastWarnTime;
      private Dictionary<TKey, BatchWrite<TKey, TEntry>> _queued;
      private LinkedList<TKey> _keys;
      private int _queuedCount;

      public TsdbWriteBatcher(
         TsdbClient<TKey, TEntry> client,
         PublicationType publishType,
         Publish publishMode,
         bool useTempStorage,
         TimeSpan writeInterval,
         int maxBatchSize,
         ITsdbLogger logger )
      {
         _queued = new Dictionary<TKey, BatchWrite<TKey, TEntry>>();
         _keys = new LinkedList<TKey>();

         _client = client;
         _writeInterval = writeInterval;
         _publishType = publishType;
         _publishMode = publishMode;
         _useTempStorage = useTempStorage;
         _maxBatchSize = maxBatchSize;
         _cts = new CancellationTokenSource();
         _logger = logger;
      }

      public int QueueCount => _queuedCount;

      public void Handle( CancellationToken cancel = default( CancellationToken ) )
      {
         WriteLoop( cancel );
      }

      public Task Write( ISerie<TKey, TEntry> serie )
      {
         lock( _sync )
         {
            var key = serie.GetKey();
            if( !_queued.TryGetValue( key, out var existingBatchWrite ) )
            {
               existingBatchWrite = new BatchWrite<TKey, TEntry>( serie );
               _queued.Add( key, existingBatchWrite );
               _keys.AddLast( key );
            }
            else
            {
               existingBatchWrite.Add( serie );
            }

            _queuedCount += serie.GetEntries().Count;

            return existingBatchWrite.Task;
         }
      }

      private List<BatchWrite<TKey, TEntry>> GetBatchesToWrite()
      {
         lock( _sync )
         {
            if( _queued.Count != 0 )
            {
               List<BatchWrite<TKey, TEntry>> batches = new List<BatchWrite<TKey, TEntry>>();
               int totalCount = 0;
               while( _keys.Count > 0 )
               {
                  var node = _keys.First;
                  _keys.RemoveFirst();

                  var batch = _queued[ node.Value ];
                  var count = batch.Count;
                  batches.Add( batch );

                  totalCount += count;
                  if( totalCount > _maxBatchSize )
                  {
                     break;
                  }
               }

               foreach( var batch in batches )
               {
                  _queued.Remove( batch.GetBatch().GetKey() );
               }
               _queuedCount -= totalCount;

               return batches;
            }
            return null;
         }
      }

      private async void WriteLoop( CancellationToken cancel )
      {
         while( !_disposed )
         {
            var writes = GetBatchesToWrite();
            if( writes != null )
            {
               try
               {
                  await _client.WriteAsync( writes.Select( x => x.GetBatch() ), _publishType, _publishMode, _useTempStorage ).ConfigureAwait( false );

                  foreach( var write in writes )
                  {
                     write.Complete();
                  }
               }
               catch( Exception e )
               {
                  _logger.Error( e, "An error ocurred while inserting batch of entries." );

                  foreach( var write in writes )
                  {
                     write.Fail( e );
                  }
               }
            }

            try
            {
               if( _queued.Count == 0 )
               {
                  await Task.Delay( _writeInterval, _cts.Token ).ConfigureAwait( false );
               }
               else if( _queuedCount >= _maxBatchSize * 10 )
               {
                  if( !_lastWarnTime.HasValue || ( DateTime.UtcNow - _lastWarnTime ) > MinWarningInterval )
                  {
                     _logger.Warn( $"There are {_queuedCount} entries queued batches waiting to be written. This is an indication that the storage is too slow to handle the ingestion." );
                     _lastWarnTime = DateTime.UtcNow;
                  }
               }
            }
            catch( OperationCanceledException )
            {
               // simply ignore
            }
         }
      }

      #region IDisposable Support

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               // TODO: dispose managed state (managed objects).
            }

            _disposed = true;
            _cts.Cancel();
         }
      }

      // This code added to correctly implement the disposable pattern.
      public void Dispose()
      {
         // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
         Dispose( true );
         // TODO: uncomment the following line if the finalizer is overridden above.
         // GC.SuppressFinalize(this);
      }

      #endregion
   }
}
