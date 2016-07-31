using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class ReadResultExtensions
   {
      public static MultiReadResult<TEntry> Combine<TEntry>( this IEnumerable<MultiReadResult<TEntry>> that )
         where TEntry : IEntry
      {
         var result = new MultiReadResult<TEntry>();
         foreach( var item in that )
         {
            result.AddOrMerge( item );
         }
         return result;
      }

      public static MultiReadResult<TEntry> Combine<TEntry>( this IEnumerable<ReadResult<TEntry>> that )
         where TEntry : IEntry
      {
         var result = new MultiReadResult<TEntry>();
         foreach( var item in that )
         {
            result.AddOrMerge( item );
         }
         return result;
      }
   }
}
