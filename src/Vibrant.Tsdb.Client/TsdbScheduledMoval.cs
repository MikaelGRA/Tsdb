using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public class TsdbScheduledMoval<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      private TsdbVolumeMoval<TKey> _moval;
      private TsdbEngine<TKey, TEntry> _engine;
      private Func<bool> _unsubscribe;

      public TsdbScheduledMoval( TsdbEngine<TKey, TEntry> engine, TsdbVolumeMoval<TKey> moval )
      {
         _engine = engine;
         _moval = moval;
      }

      public async void Execute( DateTime timestamp )
      {
         try
         {
            await _engine.Client.MoveToVolumeStorage( new[] { _moval.Id }, _engine.Work.GetDynamicMovalBatchSize(), _moval.To, _moval.StorageExpiration ).ConfigureAwait( false );
         }
         catch( Exception e )
         {
            _engine.Logger.Error( e, "An error ocurred while moving data from dynamic storage to volume storage." );
         }

         var newMoval = await _engine.Work.GetMovalAsync( _moval ).ConfigureAwait( false );

         if( newMoval != null )
         {
            _moval = newMoval;

            // schedule again
            Schedule();
         }
      }

      public void Schedule()
      {
         _unsubscribe = _engine.Scheduler.AddCommand( _moval.Timestamp, Execute );
      }

      public void Reschedule( TsdbVolumeMoval<TKey> newMoval )
      {
         _unsubscribe?.Invoke();
         _moval = newMoval;
         Schedule();
      }

      public void Unschedule()
      {
         _unsubscribe?.Invoke();
      }
   }
}
