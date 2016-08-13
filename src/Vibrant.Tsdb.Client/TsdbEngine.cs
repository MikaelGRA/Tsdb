using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public class TsdbEngine<TKey, TEntry> : IDisposable
      where TEntry : IEntry<TKey>
   {
      private EventScheduler _scheduler;
      private TsdbClient<TKey, TEntry> _client;
      private IWorkProvider<TKey> _workProvider;
      private Dictionary<TKey, TsdbScheduledMoval<TKey, TEntry>> _scheduledWork;
      private Func<bool> _unsubscribe;
      private ITsdbLogger _logger;
      private bool _disposed = false;

      public TsdbEngine( IWorkProvider<TKey> workProvider, TsdbClient<TKey, TEntry> client, ITsdbLogger logger )
      {
         _client = client;
         _workProvider = workProvider;
         _logger = logger;
         _scheduler = new EventScheduler();
         _scheduledWork = new Dictionary<TKey, TsdbScheduledMoval<TKey, TEntry>>();
      }

      public TsdbEngine( IWorkProvider<TKey> workProvider, TsdbClient<TKey, TEntry> client )
         : this( workProvider, client, NullTsdbLogger.Default )
      {
      }

      internal TsdbClient<TKey, TEntry> Client
      {
         get
         {
            return _client;
         }
      }

      internal EventScheduler Scheduler
      {
         get
         {
            return _scheduler;
         }
      }

      internal IWorkProvider<TKey> Work
      {
         get
         {
            return _workProvider;
         }
      }

      internal ITsdbLogger Logger
      {
         get
         {
            return _logger;
         }
      }

      public async Task StartAsync()
      {
         var movals = await _workProvider.GetAllMovalsAsync( DateTime.UtcNow ).ConfigureAwait( false );

         lock( _scheduledWork )
         {
            foreach( var moval in movals )
            {
               var work = new TsdbScheduledMoval<TKey, TEntry>( this, moval );
               _scheduledWork.Add( moval.Id, work );
               work.Schedule();
            }
         }

         var scheduleTime = DateTime.UtcNow + _workProvider.GetTemporaryMovalInterval();
         _unsubscribe = _scheduler.AddCommand( scheduleTime, MoveTemporaryData );

         _workProvider.MovalChangedOrAdded += WorkProvider_MovalChangedOrAdded;
         _workProvider.MovalRemoved += WorkProvider_MovalRemoved;
      }

      private async void MoveTemporaryData( DateTime timestamp )
      {
         if( !_disposed )
         {
            try
            {
               await _client.MoveFromTemporaryStorageAsync( _workProvider.GetTemporaryMovalBatchSize() ).ConfigureAwait( false );
            }
            catch( Exception e )
            {
               _logger.Error( e, "An error ocurred while moving entries from temporary to dynamic storage." );
            }

            var scheduleTime = DateTime.UtcNow + _workProvider.GetTemporaryMovalInterval();
            _unsubscribe = _scheduler.AddCommand( scheduleTime, MoveTemporaryData );
         }
      }

      private void WorkProvider_MovalRemoved( TKey id )
      {
         lock( _scheduledWork )
         {
            TsdbScheduledMoval<TKey, TEntry> work;
            if( _scheduledWork.TryGetValue( id, out work ) )
            {
               _scheduledWork.Remove( id );

               work.Unschedule();
            }
         }
      }

      private void WorkProvider_MovalChangedOrAdded( TsdbVolumeMoval<TKey> moval )
      {
         lock( _scheduledWork )
         {
            TsdbScheduledMoval<TKey, TEntry> work;
            if( _scheduledWork.TryGetValue( moval.Id, out work ) )
            {
               work.Reschedule( moval );
            }
            else
            {
               work = new TsdbScheduledMoval<TKey, TEntry>( this, moval );
               _scheduledWork.Add( moval.Id, work );
               work.Schedule();
            }
         }
      }

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {
               _scheduler.Dispose();
            }

            _unsubscribe?.Invoke();
            _workProvider.MovalChangedOrAdded -= WorkProvider_MovalChangedOrAdded;
            _workProvider.MovalRemoved -= WorkProvider_MovalRemoved;

            _disposed = true;
         }
      }

      public void Dispose()
      {
         Dispose( true );
      }
   }
}
