using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats.Helpers
{
   internal static class EntryExtensions
   {
      public static IEnumerable<EntrySplitResult<TKey, TEntry>> SplitEntriesById<TKey, TEntry>( this IEnumerable<TEntry> entries, Sort sort )
         where TEntry : IAtsEntry<TKey>
      {
         var splitEntries = new Dictionary<TKey, EntrySplitResult<TKey, TEntry>>();
         foreach( var entry in entries )
         {
            EntrySplitResult<TKey, TEntry> splitEntry;
            var id = entry.GetKey();

            if( !splitEntries.TryGetValue( id, out splitEntry ) )
            {
               splitEntry = new EntrySplitResult<TKey, TEntry>( id );
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
