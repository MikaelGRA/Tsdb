using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Exceptions;

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

      public static async Task<ITypedKey<TKey, TMeasureType>> GetTaggedKeyOrThrowAsync<TKey, TMeasureType>( this ITypedKeyStorage<TKey, TMeasureType> storage, TKey key )
         where TMeasureType : IMeasureType
      {
         var keys = new[] { key };
         var typedKeys = await storage.GetTaggedKeysAsync( keys ).ConfigureAwait( false );
         var typedKey = typedKeys.FirstOrDefault();

         if( typedKey == null || !typedKey.Key.Equals( key ) )
            throw new TsdbException( "Could not find one or more of the required keys as a typed key." );

         return typedKey;
      }

      public static async Task<IEnumerable<ITypedKey<TKey, TMeasureType>>> GetTaggedKeysOrThrowAsync<TKey, TMeasureType>( this ITypedKeyStorage<TKey, TMeasureType> storage, IEnumerable<TKey> keys )
         where TMeasureType : IMeasureType
      {
         keys = keys.ToList(); // only iterate once
         var typedKeys = await storage.GetTaggedKeysAsync( keys ).ConfigureAwait( false );
         var typedKeyDictionary = typedKeys.ToDictionary( x => x.Key );

         foreach( var key in keys )
         {
            ITypedKey<TKey, TMeasureType> typedKey;
            if( !typedKeyDictionary.TryGetValue( key, out typedKey ) )
               throw new TsdbException( "Could not find one or more of the required keys as a typed key." );

         }

         return typedKeys;
      }
   }
}
