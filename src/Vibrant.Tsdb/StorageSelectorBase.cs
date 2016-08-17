using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public abstract class StorageSelectorBase<TKey, TEntry> : IVolumeStorageSelector<TKey, TEntry>, IDynamicStorageSelector<TKey, TEntry>
     where TEntry : IEntry
   {
      protected abstract IEnumerable<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>> IterateAllDynamicStoragesFor( TKey key );

      protected abstract IEnumerable<StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>> IterateAllVolumeStoragesFor( TKey key );

      IEnumerable<StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>> IVolumeStorageSelector<TKey, TEntry>.GetStorage( TKey id, DateTime? from, DateTime? to )
      {
         var requestedFrom = from ?? DateTime.MinValue;
         var requestedTo = to ?? DateTime.MaxValue;

         foreach( var storage in IterateAllVolumeStoragesFor( id ) )
         {
            var storageFrom = storage.From ?? DateTime.MinValue;
            var storageTo = storage.To ?? DateTime.MaxValue;

            if( requestedFrom < storageTo && storageFrom < requestedTo )
            {
               yield return storage;
            }
         }
      }

      IVolumeStorage<TKey, TEntry> IVolumeStorageSelector<TKey, TEntry>.GetStorage( TKey key, TEntry entry )
      {
         foreach( var storage in IterateAllVolumeStoragesFor( key ) )
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

      IEnumerable<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>> IDynamicStorageSelector<TKey, TEntry>.GetStorage( TKey id, DateTime? from, DateTime? to )
      {
         var requestedFrom = from ?? DateTime.MinValue;
         var requestedTo = to ?? DateTime.MaxValue;

         foreach( var storage in IterateAllDynamicStoragesFor( id ) )
         {
            var storageFrom = storage.From ?? DateTime.MinValue;
            var storageTo = storage.To ?? DateTime.MaxValue;

            if( requestedFrom < storageTo && storageFrom < requestedTo )
            {
               yield return storage;
            }
         }
      }

      IDynamicStorage<TKey, TEntry> IDynamicStorageSelector<TKey, TEntry>.GetStorage( TKey key, TEntry entry )
      {
         foreach( var storage in IterateAllDynamicStoragesFor( key ) )
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
