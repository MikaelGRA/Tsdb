using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class EntryComparer<TEntry>
         where TEntry : IEntry
   {
      internal static readonly DescendingEntryComparer<TEntry> Descending = new DescendingEntryComparer<TEntry>();
      internal static readonly AscendingEntryComparer<TEntry> Ascending = new AscendingEntryComparer<TEntry>();

      public static IComparer<TEntry> GetComparer( Sort sort )
      {
         switch( sort )
         {
            case Sort.Descending:
               return Descending;
            case Sort.Ascending:
               return Ascending;
            default:
               throw new ArgumentException( "sort" );
         }
      }
   }
}
