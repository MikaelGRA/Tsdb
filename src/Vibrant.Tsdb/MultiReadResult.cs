using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MultiReadResult<TKey, TEntry> : IEnumerable<ReadResult<TKey, TEntry>>
      where TEntry : IEntry
   {
      private IDictionary<TKey, ReadResult<TKey, TEntry>> _results;

      public MultiReadResult( IDictionary<TKey, ReadResult<TKey, TEntry>> results )
      {
         _results = results;
      }

      public MultiReadResult( IEnumerable<ReadResult<TKey, TEntry>> results )
      {
         _results = results.ToDictionary( x => x.Key );
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
         if( _results.TryGetValue( result.Key, out existing ) )
         {
            existing.MergeWith( result );
         }
         else
         {
            _results.Add( result.Key, result );
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

      public MultiReadResult<TKey, TEntry> MergeWith( MultiReadResult<TKey, TEntry> other )
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

      public MultiTypedReadResult<TKey, TEntry, TMeasureType> WithTags<TMeasureType>( IDictionary<TKey, ITypedKey<TKey, TMeasureType>> conversions )
         where TMeasureType : IMeasureType
      {
         var convertedResults = new Dictionary<TKey, TypedReadResult<TKey, TEntry, TMeasureType>>();
         foreach( var result in _results.Values )
         {
            var newKey = conversions[ result.Key ];
            var newResult = result.AsTaggedResult( newKey );
            convertedResults.Add( result.Key, newResult );
         }
         return new MultiTypedReadResult<TKey, TEntry, TMeasureType>( convertedResults );
      }
   }
}
