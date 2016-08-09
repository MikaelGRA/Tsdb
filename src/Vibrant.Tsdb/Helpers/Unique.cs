using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Helpers
{
   public static class Unique
   {
      public static IEnumerable<TEntry> Ensure<TKey, TEntry>( IEnumerable<TEntry> entries, IEqualityComparer<TEntry> comparer )
         where TEntry : IEntry<TKey>
      {
         return new HashSet<TEntry>( entries, comparer );
      }
   }
}
