using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public abstract class StorageSelectorBase<TKey, TEntry> : IStorageSelector<TKey, TEntry>
     where TEntry : IEntry
   {
      protected abstract IEnumerable<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>> IterateAllStoragesFor( TKey key );

      IEnumerable<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>> IStorageSelector<TKey, TEntry>.GetStorage( TKey id, DateTime? from, DateTime? to )
      {
         var requestedFrom = from ?? DateTime.MinValue;
         var requestedTo = to ?? DateTime.MaxValue;

         foreach( var storage in IterateAllStoragesFor( id ) )
         {
            var storageFrom = storage.From ?? DateTime.MinValue;
            var storageTo = storage.To ?? DateTime.MaxValue;

            if( requestedFrom < storageTo && storageFrom < requestedTo )
            {
               yield return storage;
            }
         }
      }

      IStorage<TKey, TEntry> IStorageSelector<TKey, TEntry>.GetStorage( TKey key, TEntry entry )
      {
         foreach( var storage in IterateAllStoragesFor( key ) )
         {
            var storageFrom = storage.From ?? DateTime.MinValue;
            var storageTo = storage.To ?? DateTime.MaxValue;
            var timestamp = entry.GetTimestamp();

            if( storageFrom <= timestamp && timestamp < storageTo )
            {
               return storage.Storage;
            }
         }
         return null;
      }
   }
}
