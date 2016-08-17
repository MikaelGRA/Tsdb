using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public struct EntryKey<TKey> : IEquatable<EntryKey<TKey>>
   {
      public EntryKey( TKey key, DateTime timestamp )
      {
         Key = key;
         Timestamp = timestamp;
      }

      public TKey Key { get; set; }

      public DateTime Timestamp { get; set; }

      public bool Equals( EntryKey<TKey> other )
      {
         return Key.Equals( other.Key ) && Timestamp == other.Timestamp;
      }

      public override bool Equals( object obj )
      {
         if( obj == null ) return false;
         return obj is EntryKey<TKey> && Equals( (EntryKey<TKey>)obj );
      }

      public override int GetHashCode()
      {
         return Key.GetHashCode() + Timestamp.GetHashCode();
      }
   }
}
