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

      internal void AddOrMerge( MultiReadResult<TKey, TEntry> result )
      {
         foreach( var item in result )
         {
            AddOrMerge( item );
         }
      }

      internal void AddOrMerge( ReadResult<TKey, TEntry> result )
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

      internal void ClearEmptyResults()
      {
         foreach( var kvp in _results.ToList() )
         {
            if( kvp.Value.Entries.Count == 0 )
            {
               _results.Remove( kvp.Key );
            }
         }
      }

      internal MultiTypedReadResult<TKey, TEntry, TMeasureType> WithTags<TMeasureType>( IDictionary<TKey, ITypedKey<TKey, TMeasureType>> conversions )
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
