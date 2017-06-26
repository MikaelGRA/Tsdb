using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   internal class StorageKey<TKey, TEntry> : IEquatable<StorageKey<TKey, TEntry>>
     where TEntry : IEntry
   {
      public StorageKey( TKey key, IStorage<TKey, TEntry> storage )
      {
         Key = key;
         Storage = storage;
      }

      public TKey Key { get; set; }

      public IStorage<TKey, TEntry> Storage { get; set; }

      public bool Equals( StorageKey<TKey, TEntry> other )
      {
         return Key.Equals( other.Key ) && Storage == other.Storage;
      }

      public override bool Equals( object obj )
      {
         return Equals( obj as StorageKey<TKey, TEntry> );
      }

      public override int GetHashCode()
      {
         return Key.GetHashCode() + Storage.GetHashCode();
      }
   }
}
