using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   internal class DescendingEntryComparer<TKey, TEntry> : IComparer<TEntry>
      where TEntry : IEntry
   {
      public int Compare( TEntry x, TEntry y )
      {
         return y.GetTimestamp().CompareTo( x.GetTimestamp() );
      }
   }
}
