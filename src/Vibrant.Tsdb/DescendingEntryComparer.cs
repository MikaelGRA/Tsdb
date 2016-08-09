using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class DescendingEntryComparer<TKey, TEntry> : IComparer<TEntry>
      where TEntry : IEntry<TKey>
   {
      public int Compare( TEntry x, TEntry y )
      {
         return y.GetTimestamp().CompareTo( x.GetTimestamp() );
      }
   }
}
