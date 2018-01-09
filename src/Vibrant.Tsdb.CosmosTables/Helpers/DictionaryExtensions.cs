using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.CosmosTables.Helpers
{
   internal static class DictionaryExtensions
   {
      internal static void AddRange<TKey, TEntry>( this Dictionary<TKey, List<TEntry>> dictionary, TKey key, IEnumerable<TEntry> additions )
      {
         List<TEntry> entries;
         if( !dictionary.TryGetValue( key, out entries ) )
         {
            entries = new List<TEntry>();
            dictionary.Add( key, entries );
         }
         entries.AddRange( additions );
      }
   }
}
