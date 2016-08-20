using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class BatchWrite<TKey, TEntry>
      where TEntry : IEntry
   {
      private TaskCompletionSource<bool> _tcs;
      private Dictionary<TKey, ISerie<TKey, TEntry>> _series;
      private int _count;

      public BatchWrite()
      {
         _tcs = new TaskCompletionSource<bool>();
         _series = new Dictionary<TKey, ISerie<TKey, TEntry>>();
      }

      public void Complete()
      {
         _tcs.SetResult( true );
      }

      public void Fail( Exception e )
      {
         _tcs.SetException( e );
      }

      public void Add( ISerie<TKey, TEntry> serie )
      {
         var newEntries = serie.GetEntries();

         ISerie<TKey, TEntry> existing;
         if( _series.TryGetValue( serie.GetKey(), out existing ) )
         {
            var existingEntries = existing.GetEntries();

            var list = existingEntries as List<TEntry>;
            if( list != null )
            {
               list.AddRange( newEntries );
            }
            else
            {
               foreach( var entry in newEntries )
               {
                  existingEntries.Add( entry );
               }
            }
         }
         else
         {
            _series.Add( serie.GetKey(), serie );
         }

         _count += newEntries.Count;
      }

      public int Count
      {
         get
         {
            return _count;
         }
      }

      public ICollection<ISerie<TKey, TEntry>> GetBatch()
      {
         return _series.Values;
      }

      public Task Task
      {
         get
         {
            return _tcs.Task;
         }
      }
   }
}
