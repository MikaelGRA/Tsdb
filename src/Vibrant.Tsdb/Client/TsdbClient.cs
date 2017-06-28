using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb.Client
{
   public class TsdbClient<TKey, TEntry> : IStorage<TKey, TEntry>, ISubscribe<TKey, TEntry>
      where TEntry : IEntry
   {
      private static readonly ISerie<TKey, TEntry>[] _series = new ISerie<TKey, TEntry>[ 0 ];
      private readonly IStorageSelector<TKey, TEntry> _storageSelector;
      private readonly IPublishSubscribe<TKey, TEntry> _remotePublishSubscribe;
      private readonly ITemporaryStorage<TKey, TEntry> _temporaryStorage;
      private readonly ITsdbLogger _logger;
      private readonly DefaultPublishSubscribe<TKey, TEntry> _localPublishSubscribe;

      public TsdbClient(
         IStorageSelector<TKey, TEntry> storageSelector,
         IPublishSubscribe<TKey, TEntry> remotePublishSubscribe,
         ITemporaryStorage<TKey, TEntry> temporaryStorage,
         ITsdbLogger logger )
      {
         _storageSelector = storageSelector;
         _remotePublishSubscribe = remotePublishSubscribe;
         _temporaryStorage = temporaryStorage;
         _localPublishSubscribe = new DefaultPublishSubscribe<TKey, TEntry>( false );
         _logger = logger;
      }

      public TsdbClient(
         IStorageSelector<TKey, TEntry> storageSelector,
         IPublishSubscribe<TKey, TEntry> remotePublishSubscribe,
         ITsdbLogger logger )
         : this( storageSelector, remotePublishSubscribe, null, logger )
      {
      }

      public TsdbClient(
         IStorageSelector<TKey, TEntry> storageSelector,
         IPublishSubscribe<TKey, TEntry> remotePublishSubscribe,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
         : this( storageSelector, remotePublishSubscribe, temporaryStorage, NullTsdbLogger.Default )
      {
      }

      public TsdbClient(
         IStorageSelector<TKey, TEntry> storageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
         : this( storageSelector, null, temporaryStorage, NullTsdbLogger.Default )
      {
      }

      public TsdbClient(
         IStorageSelector<TKey, TEntry> storageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage,
         ITsdbLogger logger )
         : this( storageSelector, null, temporaryStorage, logger )
      {
      }

      public async Task MoveFromTemporaryStorageAsync( int batchSize )
      {
         if( _temporaryStorage == null )
         {
            throw new InvalidOperationException( "No temporary storage has been provided." );
         }

         var sw = Stopwatch.StartNew();
         int read = 0;
         do
         {
            // read
            var batch = await _temporaryStorage.ReadAsync( batchSize ).ConfigureAwait( false );

            // set read count
            read = batch.Sum( x => x.GetEntries().Count );

            if( read > 0 )
            {
               // write to volumetric
               var tasks = new List<Task>();
               tasks.AddRange( LookupStorages( batch.Series ).Select( c => c.Storage.WriteAsync( c.Lookups ) ) );
               await Task.WhenAll( tasks ).ConfigureAwait( false );

               // delete
               await batch.DeleteAsync().ConfigureAwait( false );

               _logger.Trace( $"Moved {read} from temporary to permanent storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
               sw.Restart();
            }
         }
         while( read != 0 );
      }

      public Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> items )
      {
         return WriteAsync( items, PublicationType.None, Publish.Nowhere, true );
      }

      public Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> items, bool useTemporaryStorageOnFailure )
      {
         return WriteAsync( items, PublicationType.None, Publish.Nowhere, useTemporaryStorageOnFailure );
      }

      public Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> items, PublicationType publicationType )
      {
         return WriteAsync( items, publicationType, publicationType != PublicationType.None ? Publish.LocallyAndRemotely : Publish.Nowhere, true );
      }

      public async Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> items, PublicationType publicationType, Publish publish, bool useTemporaryStorageOnFailure )
      {
         // ensure we only iterate the original collection once, if it is not a list or array
         if( !( items is ICollection<ISerie<TKey, TEntry>> || items is Array ) )
         {
            items = items.ToList();
         }

         var tasks = new List<Task<IEnumerable<ISerie<TKey, TEntry>>>>();
         tasks.AddRange( LookupStorages( items ).Select( c => WriteToStorageAsync( c.Storage, c.Lookups, useTemporaryStorageOnFailure ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // Only publish things that were written
         var writtenItems = tasks.SelectMany( x => x.Result );

         if( publish.HasFlag( Publish.Remotely ) )
         {
            if( _remotePublishSubscribe == null )
            {
               throw new InvalidOperationException( "No remote publish subscribe store has been provided for the TsdbClient." );
            }

            await _remotePublishSubscribe.PublishAsync( writtenItems, publicationType ).ConfigureAwait( false );
         }
         if( publish.HasFlag( Publish.Locally ) )
         {
            await _localPublishSubscribe.PublishAsync( writtenItems, publicationType ).ConfigureAwait( false );
         }
      }

      private async Task<IEnumerable<ISerie<TKey, TEntry>>> WriteToStorageAsync( IStorage<TKey, TEntry> storage, IEnumerable<ISerie<TKey, TEntry>> series, bool useTemporaryStorageOnFailure )
      {
         var sw = Stopwatch.StartNew();
         try
         {
            await storage.WriteAsync( series ).ConfigureAwait( false );
            _logger.Trace( $"Inserted {series.Sum( x => x.GetEntries().Count )} to storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
            return series;
         }
         catch( Exception e1 )
         {
            if( useTemporaryStorageOnFailure )
            {
               if( _temporaryStorage == null )
               {
                  throw new InvalidOperationException( "No temporary storage has been provided." );
               }

               try
               {
                  await _temporaryStorage.WriteAsync( series );
               }
               catch( Exception e2 )
               {
                  _logger.Error( e2, $"An error ocurred while writing to temporary storage after failing to write to permanent storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
               }
            }

            _logger.Error( e1, $"An error ocurred while writing to storage. Elapsed = {sw.ElapsedMilliseconds} ms." );

            return _series;
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupStorages( ids ).Select( c => c.Storage.DeleteAsync( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime to )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupStorages( ids, to ).Select( c => c.Storage.DeleteAsync( c.Lookups, c.To.Value ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupStorages( ids, from, to ).Select( c => c.Storage.DeleteAsync( c.Lookups, c.From.Value, c.To.Value ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids, int count )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         tasks.AddRange( ids.Select( x => ReadLatestInternal( x, count ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return tasks.Select( x => x.Result ).Combine();
      }

      private async Task<ReadResult<TKey, TEntry>> ReadLatestInternal( TKey key, int count )
      {
         var storages = _storageSelector.GetStorage( key, null, null );
         foreach( var storage in storages )
         {
            var rr = await storage.Storage.ReadLatestAsync( key, count );
            if( rr.Entries.Count > 0 )
            {
               return rr;
            }
         }

         return new ReadResult<TKey, TEntry>( key, Sort.Descending );
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupStorages( ids ).Select( c => c.Storage.ReadAsync( c.Lookups, sort ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupStorages( ids, to ).Select( c => c.Storage.ReadAsync( c.Lookups, c.To.Value, sort ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupStorages( ids, from, to ).Select( c => c.Storage.ReadAsync( c.Lookups, c.From.Value, c.To.Value, sort ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public Task<Func<Task>> SubscribeAsync( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
      {
         if( _remotePublishSubscribe == null )
         {
            throw new InvalidOperationException( "No remote publish subscribe store has been provided for the TsdbClient." );
         }

         return _remotePublishSubscribe.SubscribeAsync( ids, subscribe, callback );
      }

      public Task<Func<Task>> SubscribeToAllAsync( SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
      {
         if( _remotePublishSubscribe == null )
         {
            throw new InvalidOperationException( "No remote publish subscribe store has been provided for the TsdbClient." );
         }

         return _remotePublishSubscribe.SubscribeToAllAsync( subscribe, callback );
      }

      public Task<Func<Task>> SubscribeLocallyAsync( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
      {
         return _localPublishSubscribe.SubscribeAsync( ids, subscribe, callback );
      }

      public Task<Func<Task>> SubscribeToAllLocallyAsync( SubscriptionType subscribe, Action<Serie<TKey, TEntry>> callback )
      {
         return _localPublishSubscribe.SubscribeToAllAsync( subscribe, callback );
      }

      #region Lookup

      private IEnumerable<StorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>> LookupStorages( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         var fr = new Dictionary<StorageKey<TKey, TEntry>, StorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry>>();
         foreach( var serie in series )
         {
            var key = serie.GetKey();
            foreach( var entry in serie.GetEntries() )
            {
               var storage = _storageSelector.GetStorage( key, entry );
               var storageKey = new StorageKey<TKey, TEntry>( key, storage );

               StorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry> existingStorage;
               if( !fr.TryGetValue( storageKey, out existingStorage ) )
               {
                  existingStorage = new StorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry>( storage );
                  existingStorage.Lookups = new Serie<TKey, TEntry>( key );
                  fr.Add( storageKey, existingStorage );
               }

               existingStorage.Lookups.Entries.Add( entry );
            }
         }

         // collect series into groupings of storage
         var sr = new Dictionary<IStorage<TKey, TEntry>, StorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>>();
         StorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry> current = null;
         foreach( var kvp in fr )
         {
            var key = kvp.Key.Storage;
            if( current?.Storage != key && !sr.TryGetValue( key, out current ) )
            {
               current = new StorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>( kvp.Value.Storage );
               current.Lookups = new List<Serie<TKey, TEntry>>();
               sr.Add( key, current );
            }
            current.Lookups.Add( kvp.Value.Lookups );
         }

         return sr.Values;
      }

      private IEnumerable<StorageLookupResult<TKey, List<TKey>, TEntry>> LookupStorages( IEnumerable<TKey> ids )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>, StorageLookupResult<TKey, List<TKey>, TEntry>>();

         foreach( var id in ids )
         {
            var storages = _storageSelector.GetStorage( id, null, null );
            foreach( var storage in storages )
            {
               StorageLookupResult<TKey, List<TKey>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  existingStorage = new StorageLookupResult<TKey, List<TKey>, TEntry>( storage.Storage );
                  existingStorage.Lookups = new List<TKey>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<StorageLookupResult<TKey, List<TKey>, TEntry>> LookupStorages( IEnumerable<TKey> ids, DateTime to )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>, StorageLookupResult<TKey, List<TKey>, TEntry>>();

         foreach( var id in ids )
         {
            var storages = _storageSelector.GetStorage( id, null, to );
            foreach( var storage in storages )
            {
               StorageLookupResult<TKey, List<TKey>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  var actualTo = to;
                  if( storage.To < to )
                  {
                     actualTo = storage.To.Value;
                  }

                  existingStorage = new StorageLookupResult<TKey, List<TKey>, TEntry>( storage.Storage, null, actualTo );
                  existingStorage.Lookups = new List<TKey>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<StorageLookupResult<TKey, List<TKey>, TEntry>> LookupStorages( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>, StorageLookupResult<TKey, List<TKey>, TEntry>>();

         foreach( var id in ids )
         {
            var storages = _storageSelector.GetStorage( id, from, to );
            foreach( var storage in storages )
            {
               StorageLookupResult<TKey, List<TKey>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  var actualFrom = from;
                  if( storage.From > from )
                  {
                     actualFrom = storage.From.Value;
                  }

                  var actualTo = to;
                  if( storage.To < to )
                  {
                     actualTo = storage.To.Value;
                  }

                  existingStorage = new StorageLookupResult<TKey, List<TKey>, TEntry>( storage.Storage, actualFrom, actualTo );
                  existingStorage.Lookups = new List<TKey>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      #endregion
   }
}
