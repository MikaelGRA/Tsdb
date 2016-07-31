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
      public ReadResult( string id, List<TEntry> entries )
      {
         Id = id;
         Entries = entries;
      }

      public ReadResult( string id )
      {
         Id = id;
         Entries = new List<TEntry>();
      }

      public string Id { get; private set; }

      public List<TEntry> Entries { get; private set; }

      public ReadResult<TOutputEntry> As<TOutputEntry>()
         where TOutputEntry : IEntry
      {
         return new ReadResult<TOutputEntry>( Id, Entries.Cast<TOutputEntry>().ToList() );
      }

      public ReadResult<TEntry> MergeWith( ReadResult<TEntry> other )
      {
         Entries = MergeSort.Sort(
            collections: new[] { Entries, other.Entries },
            comparer: new EntryComparer<TEntry>(),
            resolveConflict: x => x.First() );

         return this;
      }
   }
}
