using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class EntryComparer
   {
      public static IComparer<TEntry> GetComparer<TKey, TEntry>( Sort sort )
         where TEntry : IEntry
      {
         switch( sort )
         {
            case Sort.Descending:
               return new DescendingEntryComparer<TKey, TEntry>();
            case Sort.Ascending:
               return new AscendingEntryComparer<TKey, TEntry>();
            default:
               throw new ArgumentException( "sort" );
         }
      }
   }
}
