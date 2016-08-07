using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class StorageExtensions
   {
      public static Task Delete<TStorageEntry>( this IStorage<TStorageEntry> storage, string id, DateTime from, DateTime to )
         where TStorageEntry : IEntry
      {
         return storage.Delete( new[] { id }, from, to );
      }

      public static Task Delete<TEntry>( this IDynamicStorage<TEntry> storage, string id, DateTime to )
         where TEntry : IEntry
      {
         return storage.Delete( new[] { id }, to );
      }

      public static Task Delete<TEntry>( this IStorage<TEntry> storage, string id )
         where TEntry : IEntry
      {
         return storage.Delete( new[] { id } );
      }

      public async static Task<ReadResult<TStorageEntry>> ReadLatest<TStorageEntry>( this IStorage<TStorageEntry> storage, string id )
         where TStorageEntry : IEntry
      {
         var mr = await storage.ReadLatest( new[] { id } ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TStorageEntry>> Read<TStorageEntry>( this IStorage<TStorageEntry> storage, string id, Sort sort = Sort.Descending )
         where TStorageEntry : IEntry
      {
         var mr = await storage.Read( new[] { id }, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TStorageEntry>> Read<TStorageEntry>( this IStorage<TStorageEntry> storage, string id, DateTime from, DateTime to, Sort sort = Sort.Descending )
         where TStorageEntry : IEntry
      {
         var mr = await storage.Read( new[] { id }, from, to, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }
   }
}
