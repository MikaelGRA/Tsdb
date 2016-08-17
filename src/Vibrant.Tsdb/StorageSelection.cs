using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public struct StorageSelection<TKey, TEntry, TStorage> : IEquatable<StorageSelection<TKey, TEntry, TStorage>>
      where TStorage : IStorage<TKey, TEntry>
      where TEntry : IEntry
   {
      public StorageSelection( TStorage storage, DateTime? from, DateTime? to )
      {
         Storage = storage;
         From = from;
         To = to;
      }

      public StorageSelection( TStorage storage )
      {
         Storage = storage;
         From = null;
         To = null;
      }

      public TStorage Storage { get; private set; }

      public DateTime? From { get; private set; }

      public DateTime? To { get; private set; }

      public bool Equals( StorageSelection<TKey, TEntry, TStorage> other )
      {
         return ReferenceEquals( Storage, other.Storage ) && From == other.From && To == other.To;
      }

      public override bool Equals( object obj )
      {
         if( obj == null ) return false;
         return obj is StorageSelection<TKey, TEntry, TStorage> && Equals( (StorageSelection<TKey, TEntry, TStorage>)obj );
      }

      public override int GetHashCode()
      {
         return Storage.GetHashCode() + From.GetHashCode() + To.GetHashCode();
      }
   }
}
