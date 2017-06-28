using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.ConsoleApp.Entries;

namespace Vibrant.Tsdb.ConsoleApp
{
   public class TestStorageSelector : StorageSelectorBase<BasicKey, BasicEntry>
   {
      private StorageSelection<BasicKey, BasicEntry, IStorage<BasicKey, BasicEntry>>[] _storages;

      public TestStorageSelector( StorageSelection<BasicKey, BasicEntry, IStorage<BasicKey, BasicEntry>>[] storages )
      {
         _storages = storages;
      }

      protected override IEnumerable<StorageSelection<BasicKey, BasicEntry, IStorage<BasicKey, BasicEntry>>> IterateAllStoragesFor( BasicKey key )
      {
         return _storages;
      }
   }
}
