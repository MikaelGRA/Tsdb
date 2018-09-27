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
      public ReadResult( TKey id, Sort sort, List<ReadResult<TKey, TEntry>> resultsToCompose )
      {
         Key = id;
         Sort = sort;
         Entries = MergeSort.Sort(
            collections: resultsToCompose.Select( x => x.Entries ),
            comparer: EntryComparer.GetComparer<TKey, TEntry>( Sort ),
            resolveConflict: x => x.First() );
      }

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
