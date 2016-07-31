using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MultiReadResult<TEntry> : IEnumerable<ReadResult<TEntry>>
      where TEntry : IEntry
   {
      private IDictionary<string, ReadResult<TEntry>> _results;

      public MultiReadResult( IDictionary<string, ReadResult<TEntry>> results )
      {
         _results = results;
      }

      public MultiReadResult()
      {
         _results = new Dictionary<string, ReadResult<TEntry>>();
      }

      public ReadResult<TEntry> FindResult( string id )
      {
         return _results[ id ];
      }

      public bool TryFindResult( string id, out ReadResult<TEntry> readResult )
      {
         return _results.TryGetValue( id, out readResult );
      }

      internal void AddOrMerge( MultiReadResult<TEntry> result )
      {
         foreach( var item in result )
         {
            AddOrMerge( item.Id, item );
         }
      }

      internal void AddOrMerge( string id, ReadResult<TEntry> result )
      {
         ReadResult<TEntry> existing;
         if( _results.TryGetValue( id, out existing ) )
         {
            existing.MergeWith( result );
         }
         else
         {
            _results.Add( id, result );
         }
      }

      public IEnumerator<ReadResult<TEntry>> GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }
      public MultiReadResult<TOutputEntry> As<TOutputEntry>()
         where TOutputEntry : IEntry
      {
         return new MultiReadResult<TOutputEntry>( _results.ToDictionary( x => x.Key, x => x.Value.As<TOutputEntry>() ) );
      }

      public MultiReadResult<TEntry> MergeWith( MultiReadResult<TEntry> other )
      {
         foreach( var thisResult in this )
         {
            var otherResult = other.FindResult( thisResult.Id );
            thisResult.MergeWith( otherResult );
         }
         return this;
      }
   }
}
