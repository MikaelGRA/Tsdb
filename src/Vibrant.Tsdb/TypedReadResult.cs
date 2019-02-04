using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb
{
   public class TypedReadResult<TKey, TEntry, TMeasureType> : ISortedSerie<TKey, TEntry> 
      where TEntry : IEntry
      where TMeasureType : IMeasureType
   {
      internal TypedReadResult( ITypedKey<TKey, TMeasureType> typedKey, Sort sort, List<TEntry> entries )
      {
         TypedKey = typedKey;
         Sort = sort;
         Entries = entries;
      }

      public TKey Key => TypedKey.Key;

      public ITypedKey<TKey, TMeasureType> TypedKey { get; private set; }

      public Sort Sort { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public TKey GetKey()
      {
         return Key;
      }

      public List<TEntry> GetEntries()
      {
         return Entries;
      }

      public Sort GetOrdering()
      {
         return Sort;
      }
   }
}
