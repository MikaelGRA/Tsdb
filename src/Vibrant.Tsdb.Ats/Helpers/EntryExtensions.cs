using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Helpers
{
   internal static class EntryExtensions
   {
      public static IEnumerable<EntrySplitResult<TEntry>> SplitEntriesById<TEntry>( this IEnumerable<TEntry> entries, Sort sort )
         where TEntry : IEntry
      {
         var splitEntries = new Dictionary<string, EntrySplitResult<TEntry>>();
         foreach( var entry in entries )
         {
            EntrySplitResult<TEntry> splitEntry;
            var id = entry.GetId();

            if( !splitEntries.TryGetValue( id, out splitEntry ) )
            {
               splitEntry = new EntrySplitResult<TEntry>( id );
               splitEntries.Add( id, splitEntry );
            }
            splitEntry.Insert( entry );
         }

         foreach( var splitEntry in splitEntries )
         {
            splitEntry.Value.Sort( sort );
         }

         return splitEntries.Values;
      }
   }
}
