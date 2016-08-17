using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class BatchWrite<TKey, TEntry> : IEnumerable<ISerie<TKey, TEntry>>
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
         ISerie<TKey, TEntry> existing;
         if( _series.TryGetValue( serie.GetKey(), out existing ) )
         {
            existing.Insert( serie );
         }
         else
         {
            _series.Add( serie.GetKey(), serie );
         }

         _count += serie.GetEntries().Count;
      }

      public int Count
      {
         get
         {
            return _count;
         }
      }

      public IEnumerator<ISerie<TKey, TEntry>> GetEnumerator()
      {
         return _series.Values.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _series.Values.GetEnumerator();
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
