using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class StorageExtensions
   {
      public static Task DeleteAsync<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id, DateTime from, DateTime to )
         where TEntry : IEntry<TKey>
      {
         return storage.DeleteAsync( new[] { id }, from, to );
      }

      public static Task DeleteAsync<TKey, TEntry>( this IDynamicStorage<TKey, TEntry> storage, TKey id, DateTime to )
         where TEntry : IEntry<TKey>
      {
         return storage.DeleteAsync( new[] { id }, to );
      }

      public static Task DeleteAsync<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id )
         where TEntry : IEntry<TKey>
      {
         return storage.DeleteAsync( new[] { id } );
      }

      public async static Task<ReadResult<TKey, TEntry>> ReadLatestAsync<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id )
         where TEntry : IEntry<TKey>
      {
         var mr = await storage.ReadLatestAsync( new[] { id } ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TKey, TEntry>> ReadAsync<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id, Sort sort = Sort.Descending )
         where TEntry : IEntry<TKey>
      {
         var mr = await storage.ReadAsync( new[] { id }, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }

      public async static Task<ReadResult<TKey, TEntry>> ReadAsync<TKey, TEntry>( this IStorage<TKey, TEntry> storage, TKey id, DateTime from, DateTime to, Sort sort = Sort.Descending )
         where TEntry : IEntry<TKey>
      {
         var mr = await storage.ReadAsync( new[] { id }, from, to, sort ).ConfigureAwait( false );
         return mr.FindResult( id );
      }
   }
}
