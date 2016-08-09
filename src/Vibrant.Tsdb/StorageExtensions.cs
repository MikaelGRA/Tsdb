using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class StorageExtensions
   {
      public static Task Delete<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id, DateTime from, DateTime to )
         where TEntry : IEntry<TKey>
      {
         return storage.Delete( new[] { id }, from, to );
      }

      public static Task Delete<TKey, TEntry>( this IDynamicStorage<TKey, TEntry> storage, TKey id, DateTime to )
         where TEntry : IEntry<TKey>
      {
         return storage.Delete( new[] { id }, to );
      }

      public static Task Delete<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id )
         where TEntry : IEntry<TKey>
      {
         return storage.Delete( new[] { id } );
      }

      public async static Task<ReadResult<TKey, TEntry>> ReadLatest<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id )
         where TEntry : IEntry<TKey>
      {
         var mr = await storage.ReadLatest( new[] { id } ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TKey, TEntry>> Read<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id, Sort sort = Sort.Descending )
         where TEntry : IEntry<TKey>
      {
         var mr = await storage.Read( new[] { id }, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TKey, TEntry>> Read<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id, DateTime from, DateTime to, Sort sort = Sort.Descending )
         where TEntry : IEntry<TKey>
      {
         var mr = await storage.Read( new[] { id }, from, to, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }
   }
}
