using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb
{
   public class ReadResult<TKey, TEntry> : ISerie<TKey, TEntry>
     where TEntry : IEntry
   {
      public ReadResult( TKey id, Sort sort, List<TEntry> entries )
      {
         Key = id;
         Entries = entries;
         Sort = sort;
      }

      public ReadResult( TKey id, Sort sort )
      {
         Key = id;
         Entries = new List<TEntry>();
         Sort = sort;
      }

      public TKey Key { get; private set; }

      public Sort Sort { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public ReadResult<TKey, TEntry> MergeWith( ReadResult<TKey, TEntry> other )
      {
         Entries = MergeSort.Sort(
            collections: new[] { Entries, other.Entries },
            comparer: EntryComparer.GetComparer<TKey, TEntry>( Sort ),
            resolveConflict: x => x.First() );

         return this;
      }

      public TKey GetKey()
      {
         return Key;
      }

      public ICollection<TEntry> GetEntries()
      {
         return Entries;
      }

      internal TypedReadResult<TKey, TEntry, TMeasureType> AsTaggedResult<TMeasureType>( ITypedKey<TKey, TMeasureType> typedKey )
         where TMeasureType : IMeasureType
      {
         return new TypedReadResult<TKey, TEntry, TMeasureType>( typedKey, Sort, Entries );
      }
   }
}
