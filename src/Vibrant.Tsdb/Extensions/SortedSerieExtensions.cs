using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Extensions
{
   public static class SortedSerieExtensions
   {
      public static TEntry GetLatestEntry<TKey, TEntry>( this ISortedSerie<TKey, TEntry> serie )
         where TEntry : IEntry
      {
         var sort = serie.GetOrdering();
         var entries = serie.GetEntries();
         return sort == Sort.Descending ? entries[ 0 ] : entries[ entries.Count - 1 ];
      }
   }
}
