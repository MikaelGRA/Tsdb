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
         return x.GetTimestamp() == y.GetTimestamp() && x.GetKey().Equals( y.GetKey() );
      }

      public int GetHashCode( TEntry obj )
      {
         return obj.GetTimestamp().GetHashCode() * obj.GetKey().GetHashCode();
      }
   }
}
