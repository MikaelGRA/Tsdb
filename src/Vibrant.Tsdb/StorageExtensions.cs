using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class StorageExtensions
   {
      public static Task<int> Delete<TStorageEntry>( this IStorage<TStorageEntry> storage, string id, DateTime from, DateTime to )
         where TStorageEntry : IEntry
      {
         return storage.Delete( new[] { id }, from, to );
      }

      public static Task<int> Delete<TEntry>( this IStorage<TEntry> storage, string id )
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

      public async static Task<ReadResult<TEntry>> ReadLatestAs<TEntry, TStorageEntry>( this IStorage<TStorageEntry> storage, string id ) 
         where TStorageEntry : IEntry
         where TEntry : TStorageEntry
      {
         var mr = await storage.ReadLatest( new[] { id } ).ConfigureAwait( false );
         return mr.FindResult( id ).As<TEntry>();
      }

      public async static Task<ReadResult<TStorageEntry>> Read<TStorageEntry>( this IStorage<TStorageEntry> storage, string id, Sort sort = Sort.Descending )
         where TStorageEntry : IEntry
      {
         var mr = await storage.Read( new[] { id }, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TEntry>> ReadAs<TEntry, TStorageEntry>( this IStorage<TStorageEntry> storage, string id, Sort sort = Sort.Descending ) 
         where TStorageEntry : IEntry
         where TEntry : TStorageEntry
      {
         var mr = await storage.Read( new[] { id }, sort ).ConfigureAwait( false );
         return mr.FindResult( id ).As<TEntry>();
      }

      public async static Task<ReadResult<TStorageEntry>> Read<TStorageEntry>( this IStorage<TStorageEntry> storage, string id, DateTime from, DateTime to, Sort sort = Sort.Descending )
         where TStorageEntry : IEntry
      {
         var mr = await storage.Read( new[] { id }, from, to, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TEntry>> ReadAs<TEntry, TStorageEntry>( this IStorage<TStorageEntry> storage, string id, DateTime from, DateTime to, Sort sort = Sort.Descending )
         where TStorageEntry : IEntry
         where TEntry : TStorageEntry
      {
         var mr = await storage.Read( new[] { id }, from, to, sort ).ConfigureAwait( false );
         return mr.FindResult( id ).As<TEntry>();
      }

      public async static Task<MultiReadResult<TEntry>> ReadLatestAs<TEntry, TStorageEntry>( this IStorage<TStorageEntry> storage, IEnumerable<string> ids )
         where TStorageEntry : IEntry
         where TEntry : TStorageEntry
      {
         var mr = await storage.ReadLatest( ids ).ConfigureAwait( false );
         return mr.As<TEntry>();
      }

      public async static Task<MultiReadResult<TEntry>> ReadAs<TEntry, TStorageEntry>( this IStorage<TStorageEntry> storage, IEnumerable<string> ids, Sort sort = Sort.Descending )
         where TStorageEntry : IEntry
         where TEntry : TStorageEntry
      {
         var mr = await storage.Read( ids, sort ).ConfigureAwait( false );
         return mr.As<TEntry>();
      }

      public async static Task<MultiReadResult<TEntry>> ReadAs<TEntry, TStorageEntry>( this IStorage<TStorageEntry> storage, IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
         where TStorageEntry : IEntry
         where TEntry : TStorageEntry
      {
         var mr = await storage.Read( ids, from, to, sort ).ConfigureAwait( false );
         return mr.As<TEntry>();
      }
   }
}
