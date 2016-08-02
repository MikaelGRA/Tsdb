using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class StorageExtensions
   {
      public static Task<int> Delete( this IStorage storage, string id, DateTime from, DateTime to )
      {
         return storage.Delete( new[] { id }, from, to );
      }

      public static Task<int> Delete( this IStorage storage, string id )
      {
         return storage.Delete( new[] { id } );
      }

      public async static Task<ReadResult<IEntry>> ReadLatest( this IStorage storage, string id )
      {
         var mr = await storage.ReadLatest( new[] { id } ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TEntry>> ReadLatestAs<TEntry>( this IStorage storage, string id ) where TEntry : IEntry
      {
         var mr = await storage.ReadLatest( new[] { id } ).ConfigureAwait( false );
         return mr.FindResult( id ).As<TEntry>();
      }

      public async static Task<ReadResult<IEntry>> Read( this IStorage storage, string id, Sort sort = Sort.Descending )
      {
         var mr = await storage.Read( new[] { id }, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TEntry>> ReadAs<TEntry>( this IStorage storage, string id, Sort sort = Sort.Descending ) where TEntry : IEntry
      {
         var mr = await storage.Read( new[] { id }, sort ).ConfigureAwait( false );
         return mr.FindResult( id ).As<TEntry>();
      }

      public async static Task<ReadResult<IEntry>> Read( this IStorage storage, string id, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         var mr = await storage.Read( new[] { id }, from, to, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TEntry>> ReadAs<TEntry>( this IStorage storage, string id, DateTime from, DateTime to, Sort sort = Sort.Descending ) where TEntry : IEntry
      {
         var mr = await storage.Read( new[] { id }, from, to, sort ).ConfigureAwait( false );
         return mr.FindResult( id ).As<TEntry>();
      }

      public async static Task<MultiReadResult<TEntry>> ReadLatestAs<TEntry>( this IStorage storage, IEnumerable<string> ids ) where TEntry : IEntry
      {
         var mr = await storage.ReadLatest( ids ).ConfigureAwait( false );
         return mr.As<TEntry>();
      }

      public async static Task<MultiReadResult<TEntry>> ReadAs<TEntry>( this IStorage storage, IEnumerable<string> ids, Sort sort = Sort.Descending ) where TEntry : IEntry
      {
         var mr = await storage.Read( ids, sort ).ConfigureAwait( false );
         return mr.As<TEntry>();
      }

      public async static Task<MultiReadResult<TEntry>> ReadAs<TEntry>( this IStorage storage, IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending ) where TEntry : IEntry
      {
         var mr = await storage.Read( ids, from, to, sort ).ConfigureAwait( false );
         return mr.As<TEntry>();
      }
   }
}
