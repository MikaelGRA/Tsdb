using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class TypedKeyStorageExtensions
   {
      public static async Task<ITypedKey<TKey, TMeasureType>> GetTaggedKeyAsync<TKey, TMeasureType>( this ITypedKeyStorage<TKey, TMeasureType> storage, TKey key )
         where TMeasureType : IMeasureType
      {
         var keys = new[] { key };
         var typedKeys = await storage.GetTaggedKeysAsync( keys ).ConfigureAwait( false );
         return typedKeys.FirstOrDefault();
      }
   }
}
