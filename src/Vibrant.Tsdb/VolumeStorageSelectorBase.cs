using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public abstract class VolumeStorageSelectorBase<TKey, TEntry> : IVolumeStorageSelector<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      protected abstract IEnumerable<StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>> IterateAllStoragesFor( TKey key );

      public IVolumeStorage<TKey, TEntry> GetStorage( TEntry entry )
      {
         foreach( var storage in IterateAllStoragesFor( entry.GetKey() ) )
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

      public IEnumerable<StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>> GetStorage( TKey id, DateTime? from, DateTime? to )
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
   }
}
