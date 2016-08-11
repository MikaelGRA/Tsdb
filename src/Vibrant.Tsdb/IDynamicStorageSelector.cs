using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IDynamicStorageSelector<TKey, TEntry> where TEntry : IEntry<TKey>
   {
      IDynamicStorage<TKey, TEntry> GetStorage( TKey id );

      //IDynamicStorage<TKey, TEntry> GetStorage( TKey id, DateTime from, DateTime to );

      //IDynamicStorage<TKey, TEntry> GetStorage( TEntry entry );
   }
}
