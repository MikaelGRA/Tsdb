using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Client;

namespace Vibrant.Tsdb
{
   public static class TsdbClientExtensions
   {
      public static Task WriteAsync<TKey, TEntry>( this TsdbClient<TKey, TEntry> client, ISerie<TKey, TEntry> serie, bool useTemporaryStorageOnFailure )
         where TEntry : IEntry
      {
         return client.WriteAsync( new[] { serie }, useTemporaryStorageOnFailure );
      }

      public static Task WriteAsync<TKey, TEntry>( this TsdbClient<TKey, TEntry> client, ISerie<TKey, TEntry> serie, PublicationType publicationType )
         where TEntry : IEntry
      {
         return client.WriteAsync( new[] { serie }, publicationType );
      }

      public static Task WriteAsync<TKey, TEntry>( this TsdbClient<TKey, TEntry> client, ISerie<TKey, TEntry> serie, PublicationType publicationType, Publish publish, bool useTemporaryStorageOnFailure )
         where TEntry : IEntry
      {
         return client.WriteAsync( new[] { serie }, publicationType, publish, useTemporaryStorageOnFailure );
      }

      public static async Task MoveToVolumeStorageAsync<TKey, TEntry>( this TsdbClient<TKey, TEntry> client, IEnumerable<TKey> ids, int batchSize )
         where TEntry : IEntry
      {
         var tasks = new List<Task>();
         tasks.AddRange( ids.Select( id => client.MoveToVolumeStorageAsync( id, batchSize ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public static async Task MoveToVolumeStorageAsync<TKey, TEntry>( this TsdbClient<TKey, TEntry> client, IEnumerable<TKey> ids, int batchSize, DateTime to, TimeSpan storageExpiration )
         where TEntry : IEntry
      {
         var tasks = new List<Task>();
         tasks.AddRange( ids.Select( id => client.MoveToVolumeStorageAsync( id, batchSize, to, storageExpiration ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public static Task WriteDirectlyToVolumeStorageAsync<TKey, TEntry>( this TsdbClient<TKey, TEntry> client, ISerie<TKey, TEntry> items )
         where TEntry : IEntry
      {
         return client.WriteDirectlyToVolumeStorageAsync( new[] { items } );
      }

      public static Task<Func<Task>> SubscribeLocallyAsync<TKey, TEntry>( this TsdbClient<TKey, TEntry> client, TKey id, SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
         where TEntry : IEntry
      {
         return client.SubscribeLocallyAsync( new[] { id }, subscribe, callback );
      }
   }
}
