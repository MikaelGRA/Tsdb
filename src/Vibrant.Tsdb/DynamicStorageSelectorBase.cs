//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;

//namespace Vibrant.Tsdb
//{
//   public abstract class DynamicStorageSelectorBase<TKey, TEntry> : IDynamicStorageSelector<TKey, TEntry>
//      where TEntry : IEntry
//   {
//      protected abstract IEnumerable<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>> IterateAllStoragesFor( TKey key );

//      public IDynamicStorage<TKey, TEntry> GetStorage( TKey key, TEntry entry )
//      {
//         foreach( var storage in IterateAllStoragesFor( key ) )
//         {
//            var storageFrom = storage.From ?? DateTime.MinValue;
//            var storageTo = storage.To ?? DateTime.MaxValue;
//            var timestamp = entry.GetTimestamp();

//            if( storageFrom <= timestamp && timestamp < storageTo )
//            {
//               return storage.Storage;
//            }
//         }
//         return null;
//      }

//      public IEnumerable<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>> GetStorage( TKey id, DateTime? from, DateTime? to )
//      {
//         var requestedFrom = from ?? DateTime.MinValue;
//         var requestedTo = to ?? DateTime.MaxValue;

//         foreach( var storage in IterateAllStoragesFor( id ) )
//         {
//            var storageFrom = storage.From ?? DateTime.MinValue;
//            var storageTo = storage.To ?? DateTime.MaxValue;

//            if( requestedFrom < storageTo && storageFrom < requestedTo )
//            {
//               yield return storage;
//            }
//         }
//      }
//   }
//}
