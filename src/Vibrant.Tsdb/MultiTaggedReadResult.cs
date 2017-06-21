using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MultiTaggedReadResult<TKey, TEntry> : IEnumerable<TaggedReadResult<TKey, TEntry>>
     where TEntry : IEntry
   {
      private IDictionary<TKey, TaggedReadResult<TKey, TEntry>> _results;

      public MultiTaggedReadResult( IDictionary<TKey, TaggedReadResult<TKey, TEntry>> results )
      {
         _results = results;
      }

      public MultiTaggedReadResult( IEnumerable<TaggedReadResult<TKey, TEntry>> results )
      {
         _results = results.ToDictionary( x => x.Key );
      }

      public MultiTaggedReadResult()
      {
         _results = new Dictionary<TKey, TaggedReadResult<TKey, TEntry>>();
      }

      public TaggedReadResult<TKey, TEntry> FindResult( TKey id )
      {
         return _results[ id ];
      }

      public bool TryFindResult( TKey id, out TaggedReadResult<TKey, TEntry> readResult )
      {
         return _results.TryGetValue( id, out readResult );
      }

      public void AddOrMerge( MultiTaggedReadResult<TKey, TEntry> result )
      {
         foreach( var item in result )
         {
            AddOrMerge( item );
         }
      }

      public void AddOrMerge( TaggedReadResult<TKey, TEntry> result )
      {
         TaggedReadResult<TKey, TEntry> existing;
         if( _results.TryGetValue( result.Key, out existing ) )
         {
            existing.MergeWith( result );
         }
         else
         {
            _results.Add( result.Key, result );
         }
      }

      public IEnumerator<TaggedReadResult<TKey, TEntry>> GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }

      public MultiTaggedReadResult<TKey, TEntry> MergeWith( MultiTaggedReadResult<TKey, TEntry> other )
      {
         foreach( var thisResult in this )
         {
            var otherResult = other.FindResult( thisResult.Key );
            thisResult.MergeWith( otherResult );
         }
         return this;
      }

      public MultiReadResult<TKey, TEntry> MergeInto( MultiReadResult<TKey, TEntry> other )
      {
         foreach( var otherResult in other )
         {
            var thisResult = FindResult( otherResult.Key );
            otherResult.MergeWith( thisResult );
         }
         return this;
      }
   }
}
