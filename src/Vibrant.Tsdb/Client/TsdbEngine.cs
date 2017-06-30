//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;

//namespace Vibrant.Tsdb.Client
//{
//   public class TsdbEngine<TKey, TEntry> : IDisposable
//      where TEntry : IEntry
//   {
//      private EventScheduler _scheduler;
//      private TsdbClient<TKey, TEntry> _client;
//      private IWorkProvider<TKey> _workProvider;
//      private Func<bool> _unsubscribe;
//      private ITsdbLogger _logger;
//      private bool _disposed = false;

//      public TsdbEngine( IWorkProvider<TKey> workProvider, TsdbClient<TKey, TEntry> client, ITsdbLogger logger )
//      {
//         _client = client;
//         _workProvider = workProvider;
//         _logger = logger;
//         _scheduler = new EventScheduler();
//      }

//      public TsdbEngine( IWorkProvider<TKey> workProvider, TsdbClient<TKey, TEntry> client )
//         : this( workProvider, client, NullTsdbLogger.Default )
//      {
//      }

//      internal TsdbClient<TKey, TEntry> Client
//      {
//         get
//         {
//            return _client;
//         }
//      }

//      internal EventScheduler Scheduler
//      {
//         get
//         {
//            return _scheduler;
//         }
//      }

//      internal IWorkProvider<TKey> Work
//      {
//         get
//         {
//            return _workProvider;
//         }
//      }

//      internal ITsdbLogger Logger
//      {
//         get
//         {
//            return _logger;
//         }
//      }

//      public async Task StartAsync()
//      {
//         var scheduleTime = DateTime.UtcNow + _workProvider.GetTemporaryMovalInterval();
//         _unsubscribe = _scheduler.AddCommand( scheduleTime, MoveTemporaryData );
//      }

//      private async void MoveTemporaryData( DateTime timestamp )
//      {
//         if( !_disposed )
//         {
//            try
//            {
//               await _client.MoveFromTemporaryStorageAsync( _workProvider.GetTemporaryMovalBatchSize() ).ConfigureAwait( false );
//            }
//            catch( Exception e )
//            {
//               _logger.Error( e, "An error ocurred while moving entries from temporary to permanent storage." );
//            }

//            var scheduleTime = DateTime.UtcNow + _workProvider.GetTemporaryMovalInterval();
//            _unsubscribe = _scheduler.AddCommand( scheduleTime, MoveTemporaryData );
//         }
//      }

//      protected virtual void Dispose( bool disposing )
//      {
//         if( !_disposed )
//         {
//            if( disposing )
//            {
//               _scheduler.Dispose();
//            }

//            _unsubscribe?.Invoke();

//            _disposed = true;
//         }
//      }

//      public void Dispose()
//      {
//         Dispose( true );
//      }
//   }
//}
