using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb
{
   public class ReadResult<TEntry>
     where TEntry : IEntry
   {
      public ReadResult( string id, Sort sort, List<TEntry> entries )
      {
         Id = id;
         Entries = entries;
         Sort = sort;
      }

      public ReadResult( string id, Sort sort )
      {
         Id = id;
         Entries = new List<TEntry>();
         Sort = sort;
      }

      public string Id { get; private set; }

      public Sort Sort { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public ReadResult<TOutputEntry> As<TOutputEntry>()
         where TOutputEntry : IEntry
      {
         return new ReadResult<TOutputEntry>( Id, Sort, Entries.Cast<TOutputEntry>().ToList() );
      }

      public ReadResult<TEntry> MergeWith( ReadResult<TEntry> other )
      {
         Entries = MergeSort.Sort(
            collections: new[] { Entries, other.Entries },
            comparer: EntryComparer.GetComparer<TEntry>( Sort ),
            resolveConflict: x => x.First() );

         return this;
      }
   }
}
