using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.ConsoleApp.Entries;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class TestDynamicStorageSelector : DynamicStorageSelectorBase<BasicKey, BasicEntry>
   {
      private StorageSelection<BasicKey, BasicEntry, IDynamicStorage<BasicKey, BasicEntry>>[] _storages;

      public TestDynamicStorageSelector( StorageSelection<BasicKey, BasicEntry, IDynamicStorage<BasicKey, BasicEntry>>[] storages )
      {
         _storages = storages;
      }

      protected override IEnumerable<StorageSelection<BasicKey, BasicEntry, IDynamicStorage<BasicKey, BasicEntry>>> IterateAllStoragesFor( BasicKey key )
      {
         return _storages;
      }
   }
}
