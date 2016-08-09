using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MultiReadResult<TKey, TEntry> : IEnumerable<ReadResult<TKey, TEntry>>
      where TEntry : IEntry<TKey>
   {
      private IDictionary<TKey, ReadResult<TKey, TEntry>> _results;

      public MultiReadResult( IDictionary<TKey, ReadResult<TKey, TEntry>> results )
      {
         _results = results;
      }

      public MultiReadResult( IEnumerable<ReadResult<TKey, TEntry>> results )
      {
         _results = results.ToDictionary( x => x.Id );
      }

      public MultiReadResult()
      {
         _results = new Dictionary<TKey, ReadResult<TKey, TEntry>>();
      }

      public ReadResult<TKey, TEntry> FindResult( TKey id )
      {
         return _results[ id ];
      }

      public bool TryFindResult( TKey id, out ReadResult<TKey, TEntry> readResult )
      {
         return _results.TryGetValue( id, out readResult );
      }

      public void AddOrMerge( MultiReadResult<TKey, TEntry> result )
      {
         foreach( var item in result )
         {
            AddOrMerge( item );
         }
      }

      public void AddOrMerge( ReadResult<TKey, TEntry> result )
      {
         ReadResult<TKey, TEntry> existing;
         if( _results.TryGetValue( result.Id, out existing ) )
         {
            existing.MergeWith( result );
         }
         else
         {
            _results.Add( result.Id, result );
         }
      }

      public IEnumerator<ReadResult<TKey, TEntry>> GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }
      public MultiReadResult<TKey, TOutputEntry> As<TOutputEntry>()
         where TOutputEntry : IEntry<TKey>
      {
         return new MultiReadResult<TKey, TOutputEntry>( _results.ToDictionary( x => x.Key, x => x.Value.As<TOutputEntry>() ) );
      }

      public MultiReadResult<TKey, TEntry> MergeWith( MultiReadResult<TKey, TEntry> other )
      {
         foreach( var thisResult in this )
         {
            var otherResult = other.FindResult( thisResult.Id );
            thisResult.MergeWith( otherResult );
         }
         return this;
      }

      public MultiReadResult<TKey, TEntry> MergeInto( MultiReadResult<TKey, TEntry> other )
      {
         foreach( var otherResult in other )
         {
            var thisResult = FindResult( otherResult.Id );
            otherResult.MergeWith( thisResult );
         }
         return this;
      }
   }
}
