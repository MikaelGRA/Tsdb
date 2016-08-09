using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb
{
   public class ReadResult<TKey, TEntry>
     where TEntry : IEntry<TKey>
   {
      public ReadResult( TKey id, Sort sort, List<TEntry> entries )
      {
         Id = id;
         Entries = entries;
         Sort = sort;
      }

      public ReadResult( TKey id, Sort sort )
      {
         Id = id;
         Entries = new List<TEntry>();
         Sort = sort;
      }

      public TKey Id { get; private set; }

      public Sort Sort { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public ReadResult<TKey, TOutputEntry> As<TOutputEntry>()
         where TOutputEntry : IEntry<TKey>
      {
         return new ReadResult<TKey, TOutputEntry>( Id, Sort, Entries.Cast<TOutputEntry>().ToList() );
      }

      public ReadResult<TKey, TEntry> MergeWith( ReadResult<TKey, TEntry> other )
      {
         Entries = MergeSort.Sort(
            collections: new[] { Entries, other.Entries },
            comparer: EntryComparer.GetComparer<TKey, TEntry>( Sort ),
            resolveConflict: x => x.First() );

         return this;
      }
   }
}
