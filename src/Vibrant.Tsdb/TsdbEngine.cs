using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbEngine<TKey, TEntry> : IDisposable
      where TEntry : IEntry<TKey>
   {
      public event EventHandler<ExceptionEventArgs> MoveTemporaryDataFailed;
      public event EventHandler<ExceptionEventArgs> MoveToVolumeStorageFailed;

      private EventScheduler _scheduler;
      private TsdbClient<TKey, TEntry> _client;
      private IWorkProvider<TKey> _workProvider;
      private Dictionary<TKey, TsdbScheduledMoval<TKey, TEntry>> _scheduledWork;
      private Func<bool> _unsubscribe;
      private bool _disposed = false;

      public TsdbEngine( IWorkProvider<TKey> workProvider, TsdbClient<TKey, TEntry> client )
      {
         _client = client;
         _workProvider = workProvider;
         _scheduler = new EventScheduler();
         _scheduledWork = new Dictionary<TKey, TsdbScheduledMoval<TKey, TEntry>>();
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
               await _client.MoveFromTemporaryStorage( _workProvider.GetTemporaryMovalBatchSize() ).ConfigureAwait( false );
            }
            catch( Exception e )
            {
               RaiseMoveTemporaryDataFailed( e );
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

      internal void RaiseMoveTemporaryDataFailed( Exception exception )
      {
         MoveTemporaryDataFailed?.Invoke( this, new ExceptionEventArgs( exception ) );
      }

      internal void RaiseMoveToVolumeStorageFailed( Exception exception )
      {
         MoveToVolumeStorageFailed?.Invoke( this, new ExceptionEventArgs( exception ) );
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
