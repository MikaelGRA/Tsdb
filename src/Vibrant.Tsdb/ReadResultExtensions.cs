using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class ReadResultExtensions
   {
      public static MultiReadResult<TKey, TEntry> Combine<TKey, TEntry>( this IEnumerable<MultiReadResult<TKey, TEntry>> that )
         where TEntry : IEntry<TKey>
      {
         var result = new MultiReadResult<TKey, TEntry>();
         foreach( var item in that )
         {
            result.AddOrMerge( item );
         }
         return result;
      }

      public static MultiReadResult<TKey, TEntry> Combine<TKey, TEntry>( this IEnumerable<ReadResult<TKey, TEntry>> that )
         where TEntry : IEntry<TKey>
      {
         var result = new MultiReadResult<TKey, TEntry>();
         foreach( var item in that )
         {
            result.AddOrMerge( item );
         }
         return result;
      }
   }
}
