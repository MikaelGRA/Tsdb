using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbEngine<TEntry> : IDisposable
      where TEntry : IEntry
   {
      private EventScheduler _scheduler;
      private TsdbClient<TEntry> _client;
      private IWorkProvider _workProvider;
      private Dictionary<string, TsdbScheduledMoval<TEntry>> _scheduledWork;
      private bool _disposed = false;

      public TsdbEngine( IWorkProvider workProvider, TsdbClient<TEntry> client )
      {
         _client = client;
         _workProvider = workProvider;
         _scheduler = new EventScheduler();
         _scheduledWork = new Dictionary<string, TsdbScheduledMoval<TEntry>>();
      }

      internal TsdbClient<TEntry> Client
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

      internal IWorkProvider Work
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
               var work = new TsdbScheduledMoval<TEntry>( this, moval );
               _scheduledWork.Add( moval.Id, work );
               work.Schedule();
            }
         }

         _workProvider.MovalChangedOrAdded += WorkProvider_MovalChangedOrAdded;
         _workProvider.MovalRemoved += WorkProvider_MovalRemoved;
      }

      private void WorkProvider_MovalRemoved( string id )
      {
         lock( _scheduledWork )
         {
            TsdbScheduledMoval<TEntry> work;
            if( _scheduledWork.TryGetValue( id, out work ) )
            {
               _scheduledWork.Remove( id );

               work.Unschedule();
            }
         }
      }

      private void WorkProvider_MovalChangedOrAdded( TsdbVolumeMoval moval )
      {
         lock( _scheduledWork )
         {
            TsdbScheduledMoval<TEntry> work;
            if( _scheduledWork.TryGetValue( moval.Id, out work ) )
            {
               work.Reschedule( moval );
            }
            else
            {
               work = new TsdbScheduledMoval<TEntry>( this, moval );
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
