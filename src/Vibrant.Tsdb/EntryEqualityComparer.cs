using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class EntryEqualityComparer<TKey, TEntry> : IEqualityComparer<TEntry>
     where TEntry : IEntry
   {
      public bool Equals( TEntry x, TEntry y )
      {
         return x.GetTimestamp() == y.GetTimestamp();
      }

      public int GetHashCode( TEntry obj )
      {
         return obj.GetTimestamp().GetHashCode();
      }
   }
}
