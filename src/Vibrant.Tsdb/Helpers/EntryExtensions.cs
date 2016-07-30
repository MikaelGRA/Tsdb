using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Helpers
{
   public static class EntryExtensions
   {
      public static bool TryGetFromAndTo<TEntry>( this IEnumerable<TEntry> entries, out DateTime from, out DateTime to )
         where TEntry : IEntry
      {
         from = default( DateTime );
         to = default( DateTime );
         bool first = true;

         foreach( var entry in entries )
         {
            var timestamp = entry.GetTimestamp();
            if( first )
            {
               first = false;
               from = timestamp;
               to = timestamp;
            }
            else
            {
               if( timestamp < from )
               {
                  from = timestamp;
               }
               if( timestamp > to )
               {
                  to = timestamp;
               }
            }
         }

         return !first;
      }
   }
}
