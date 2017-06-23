using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class MultiTypedReadResult<TKey, TEntry, TMeasureType> : IEnumerable<TypedReadResult<TKey, TEntry, TMeasureType>>
      where TEntry : IEntry
      where TMeasureType : IMeasureType
   {
      private IDictionary<TKey, TypedReadResult<TKey, TEntry, TMeasureType>> _results;

      public MultiTypedReadResult( IDictionary<TKey, TypedReadResult<TKey, TEntry, TMeasureType>> results )
      {
         _results = results;
      }

      public TypedReadResult<TKey, TEntry, TMeasureType> FindResult( TKey id )
      {
         return _results[ id ];
      }

      public bool TryFindResult( TKey id, out TypedReadResult<TKey, TEntry, TMeasureType> readResult )
      {
         return _results.TryGetValue( id, out readResult );
      }

      public IEnumerator<TypedReadResult<TKey, TEntry, TMeasureType>> GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }

      IEnumerator IEnumerable.GetEnumerator()
      {
         return _results.Values.GetEnumerator();
      }
   }
}
