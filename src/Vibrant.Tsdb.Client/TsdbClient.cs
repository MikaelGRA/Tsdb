using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Client
{
   public class TsdbClient<TKey, TEntry> : IStorage<TKey, TEntry>, ISubscribe<TKey, TEntry>
      where TEntry : IEntry
   {
      private static readonly ISerie<TKey, TEntry>[] _series = new ISerie<TKey, TEntry>[ 0 ];
      private IDynamicStorageSelector<TKey, TEntry> _dynamicStorageSelector;
      private IVolumeStorageSelector<TKey, TEntry> _volumeStorageSelector;
      private IPublishSubscribe<TKey, TEntry> _remotePublishSubscribe;
      private ITemporaryStorage<TKey, TEntry> _temporaryStorage;
      private ITsdbLogger _logger;
      private DefaultPublishSubscribe<TKey, TEntry> _localPublishSubscribe;
      private MigrationProvider<TKey, TEntry> _migrations;

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector,
         IPublishSubscribe<TKey, TEntry> remotePublishSubscribe,
         ITemporaryStorage<TKey, TEntry> temporaryStorage,
         ITsdbLogger logger )
      {
         _dynamicStorageSelector = dynamicStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _remotePublishSubscribe = remotePublishSubscribe;
         _temporaryStorage = temporaryStorage;
         _localPublishSubscribe = new DefaultPublishSubscribe<TKey, TEntry>( false );
         _logger = logger;
         _migrations = new MigrationProvider<TKey, TEntry>( _dynamicStorageSelector, _volumeStorageSelector );
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector,
         IPublishSubscribe<TKey, TEntry> remotePublishSubscribe,
         ITsdbLogger logger )
         : this( dynamicStorageSelector, volumeStorageSelector, remotePublishSubscribe, null, logger )
      {
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector,
         IPublishSubscribe<TKey, TEntry> remotePublishSubscribe,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
         : this( dynamicStorageSelector, volumeStorageSelector, remotePublishSubscribe, temporaryStorage, NullTsdbLogger.Default )
      {
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
         : this( dynamicStorageSelector, volumeStorageSelector, null, temporaryStorage, NullTsdbLogger.Default )
      {
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
         : this( dynamicStorageSelector, null, null, temporaryStorage, NullTsdbLogger.Default )
      {
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage,
         ITsdbLogger logger )
         : this( dynamicStorageSelector, volumeStorageSelector, null, temporaryStorage, logger )
      {
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage,
         ITsdbLogger logger )
         : this( dynamicStorageSelector, null, null, temporaryStorage, logger )
      {
      }

      public async Task MoveToVolumeStorageAsync( TKey id, int batchSize )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }


         foreach( var migration in _migrations.Provide( id, null, null ).Reverse() )
         {
            var dynamic = migration.Dynamic;
            var volume = migration.Volume;

            var readSegmentedAsync = dynamic is IReversableDynamicStorage<TKey, TEntry>
               ? ( (IReversableDynamicStorage<TKey, TEntry>)dynamic ).ReadReverseSegmentedAsync
               : (Func<TKey, DateTime?, DateTime?, int, IContinuationToken, Task<SegmentedReadResult<TKey, TEntry>>>)dynamic.ReadSegmentedAsync;

            var sw = Stopwatch.StartNew();

            IContinuationToken token = null;
            do
            {
               var segment = await readSegmentedAsync( id, migration.From, migration.To, batchSize, token ).ConfigureAwait( false );
               if( segment.Entries.Count > 0 )
               {
                  await volume.WriteAsync( segment ).ConfigureAwait( false );
                  await segment.DeleteAsync().ConfigureAwait( false );

                  _logger.Info( $"Moved {segment.Entries.Count} from dynamic to volume storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
               }
               token = segment.ContinuationToken;

               sw.Restart();
            }
            while( token.HasMore );
         }
      }

      public async Task MoveToVolumeStorageAsync( TKey id, int batchSize, DateTime to, TimeSpan storageExpiration )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }


         foreach( var migration in _migrations.Provide( id, null, to ).Reverse() )
         {
            // barrier, dont migrate items from stores that has not been in use for a while
            if( migration.To.HasValue )
            {
               if( to - migration.To.Value > storageExpiration )
               {
                  break;
               }
            }

            var dynamic = migration.Dynamic;
            var volume = migration.Volume;

            var readSegmentedAsync = dynamic is IReversableDynamicStorage<TKey, TEntry>
               ? ( (IReversableDynamicStorage<TKey, TEntry>)dynamic ).ReadReverseSegmentedAsync
               : (Func<TKey, DateTime?, DateTime?, int, IContinuationToken, Task<SegmentedReadResult<TKey, TEntry>>>)dynamic.ReadSegmentedAsync;

            var sw = Stopwatch.StartNew();

            IContinuationToken token = null;
            do
            {
               var segment = await readSegmentedAsync( id, migration.From, migration.To, batchSize, token ).ConfigureAwait( false );
               if( segment.Entries.Count > 0 )
               {
                  await volume.WriteAsync( segment ).ConfigureAwait( false );
                  await segment.DeleteAsync().ConfigureAwait( false );

                  _logger.Info( $"Moved {segment.Entries.Count} from dynamic to volume storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
               }
               token = segment.ContinuationToken;

               sw.Restart();
            }
            while( token.HasMore );
         }
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
               tasks.AddRange( LookupDynamicStorages( batch.Series ).Select( c => c.Storage.WriteAsync( c.Lookups ) ) );
               await Task.WhenAll( tasks ).ConfigureAwait( false );

               // delete
               await batch.DeleteAsync().ConfigureAwait( false );

               _logger.Info( $"Moved {read} from temporary to dynamic storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
               sw.Restart();
            }
         }
         while( read != 0 );
      }

      public async Task WriteDirectlyToVolumeStorageAsync( IEnumerable<ISerie<TKey, TEntry>> items )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }
         var sw = Stopwatch.StartNew();

         var tasks = new List<Task>();
         tasks.AddRange( LookupVolumeStorages( items ).Select( c => c.Storage.WriteAsync( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         _logger.Info( $"Wrote {items.Count()} directly to dynamic storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
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
         tasks.AddRange( LookupDynamicStorages( items ).Select( c => WriteToDynamicStorageAsync( c.Storage, c.Lookups, useTemporaryStorageOnFailure ) ) );
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

      private async Task<IEnumerable<ISerie<TKey, TEntry>>> WriteToDynamicStorageAsync( IDynamicStorage<TKey, TEntry> storage, IEnumerable<ISerie<TKey, TEntry>> series, bool useTemporaryStorageOnFailure )
      {
         var sw = Stopwatch.StartNew();
         try
         {
            await storage.WriteAsync( series ).ConfigureAwait( false );
            _logger.Info( $"Wrote {series.Sum( x => x.GetEntries().Count )} to dynamic storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
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
                  _temporaryStorage.WriteAsync( series );
               }
               catch( Exception e2 )
               {
                  _logger.Error( e2, $"An error ocurred while writing to temporary storage after failing to write to dynamic storage. Elapsed = {sw.ElapsedMilliseconds} ms." );
               }
            }

            _logger.Error( e1, $"An error ocurred while writing to dynamic storage. Elapsed = {sw.ElapsedMilliseconds} ms." );

            return _series;
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.DeleteAsync( c.Lookups ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.DeleteAsync( c.Lookups ) ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupDynamicStorages( ids, from, to ).Select( c => c.Storage.DeleteAsync( c.Lookups, c.From.Value, c.To.Value ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.DeleteAsync( c.Lookups, from, to ) ) );
         }
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
         var dynamics = _dynamicStorageSelector.GetStorage( key, null, null );
         foreach( var dynamic in dynamics )
         {
            var rr = await dynamic.Storage.ReadLatestAsync( key, count );
            if( rr.Entries.Count > 0 )
            {
               return rr;
            }
         }

         if( _volumeStorageSelector != null )
         {
            var volumes = _volumeStorageSelector.GetStorage( key, null, null );
            foreach( var volume in volumes )
            {
               var rr = await volume.Storage.ReadLatestAsync( key, count );
               if( rr.Entries.Count > 0 )
               {
                  return rr;
               }
            }
         }

         return new ReadResult<TKey, TEntry>( key, Sort.Descending );
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.ReadAsync( c.Lookups, sort ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadAsync( c.Lookups, sort ) ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids, from, to ).Select( c => c.Storage.ReadAsync( c.Lookups, c.From.Value, c.To.Value, sort ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadAsync( c.Lookups, c.From.Value, c.To.Value, sort ) ) );
         }
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

      private IEnumerable<VolumeStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>> LookupVolumeStorages( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         var fr = new Dictionary<StorageKey<TKey, TEntry>, VolumeStorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry>>();

         foreach( var serie in series )
         {
            var key = serie.GetKey();

            foreach( var entry in serie.GetEntries() )
            {
               var storage = _volumeStorageSelector.GetStorage( key, entry );
               if( storage != null )
               {
                  var storageKey = new StorageKey<TKey, TEntry>( key, storage );

                  VolumeStorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry> existingStorage;
                  if( !fr.TryGetValue( storageKey, out existingStorage ) )
                  {
                     existingStorage = new VolumeStorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry>( storage );
                     existingStorage.Lookups = new Serie<TKey, TEntry>( key );
                     fr.Add( storageKey, existingStorage );
                  }

                  existingStorage.Lookups.Entries.Add( entry );
               }
            }
         }

         // collect series into groupings of storage
         var sr = new Dictionary<IStorage<TKey, TEntry>, VolumeStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>>();
         VolumeStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry> current = null;
         foreach( var kvp in fr )
         {
            var key = kvp.Key.Storage;
            if( current?.Storage != key && !sr.TryGetValue( key, out current ) )
            {
               current = new VolumeStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>( kvp.Value.Storage );
               current.Lookups = new List<Serie<TKey, TEntry>>();
               sr.Add( key, current );
            }
            current.Lookups.Add( kvp.Value.Lookups );
         }

         return sr.Values;
      }

      private IEnumerable<VolumeStorageLookupResult<TKey, List<TKey>, TEntry>> LookupVolumeStorages( IEnumerable<TKey> ids )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>, VolumeStorageLookupResult<TKey, List<TKey>, TEntry>>();

         foreach( var id in ids )
         {
            var storages = _volumeStorageSelector.GetStorage( id, null, null );
            foreach( var storage in storages )
            {
               VolumeStorageLookupResult<TKey, List<TKey>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  existingStorage = new VolumeStorageLookupResult<TKey, List<TKey>, TEntry>( storage.Storage );
                  existingStorage.Lookups = new List<TKey>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<VolumeStorageLookupResult<TKey, List<TKey>, TEntry>> LookupVolumeStorages( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IVolumeStorage<TKey, TEntry>>, VolumeStorageLookupResult<TKey, List<TKey>, TEntry>>();

         foreach( var id in ids )
         {
            var storages = _volumeStorageSelector.GetStorage( id, from, to );
            foreach( var storage in storages )
            {
               VolumeStorageLookupResult<TKey, List<TKey>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  var actualFrom = storage.From ?? from;
                  if( actualFrom < from )
                  {
                     actualFrom = from;
                  }

                  var actualTo = storage.To ?? to;
                  if( actualTo > to )
                  {
                     actualTo = to;
                  }

                  existingStorage = new VolumeStorageLookupResult<TKey, List<TKey>, TEntry>( storage.Storage, actualFrom, actualTo );
                  existingStorage.Lookups = new List<TKey>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>> LookupDynamicStorages( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         var fr = new Dictionary<StorageKey<TKey, TEntry>, DynamicStorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry>>();
         foreach( var serie in series )
         {
            var key = serie.GetKey();
            foreach( var entry in serie.GetEntries() )
            {
               var storage = _dynamicStorageSelector.GetStorage( key, entry );
               var storageKey = new StorageKey<TKey, TEntry>( key, storage );

               DynamicStorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry> existingStorage;
               if( !fr.TryGetValue( storageKey, out existingStorage ) )
               {
                  existingStorage = new DynamicStorageLookupResult<TKey, Serie<TKey, TEntry>, TEntry>( storage );
                  existingStorage.Lookups = new Serie<TKey, TEntry>( key );
                  fr.Add( storageKey, existingStorage );
               }

               existingStorage.Lookups.Entries.Add( entry );
            }
         }

         // collect series into groupings of storage
         var sr = new Dictionary<IStorage<TKey, TEntry>, DynamicStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>>();
         DynamicStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry> current = null;
         foreach( var kvp in fr )
         {
            var key = kvp.Key.Storage;
            if( current?.Storage != key && !sr.TryGetValue( key, out current ) )
            {
               current = new DynamicStorageLookupResult<TKey, List<Serie<TKey, TEntry>>, TEntry>( kvp.Value.Storage );
               current.Lookups = new List<Serie<TKey, TEntry>>();
               sr.Add( key, current );
            }
            current.Lookups.Add( kvp.Value.Lookups );
         }

         return sr.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<TKey, List<TKey>, TEntry>> LookupDynamicStorages( IEnumerable<TKey> ids )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>, DynamicStorageLookupResult<TKey, List<TKey>, TEntry>>();

         foreach( var id in ids )
         {
            var storages = _dynamicStorageSelector.GetStorage( id, null, null );
            foreach( var storage in storages )
            {
               DynamicStorageLookupResult<TKey, List<TKey>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  existingStorage = new DynamicStorageLookupResult<TKey, List<TKey>, TEntry>( storage.Storage );
                  existingStorage.Lookups = new List<TKey>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<TKey, List<TKey>, TEntry>> LookupDynamicStorages( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>, DynamicStorageLookupResult<TKey, List<TKey>, TEntry>>();

         foreach( var id in ids )
         {
            var storages = _dynamicStorageSelector.GetStorage( id, from, to );
            foreach( var storage in storages )
            {
               DynamicStorageLookupResult<TKey, List<TKey>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  var actualFrom = storage.From ?? from;
                  if( actualFrom < from )
                  {
                     actualFrom = from;
                  }

                  var actualTo = storage.To ?? to;
                  if( actualTo > to )
                  {
                     actualTo = to;
                  }

                  existingStorage = new DynamicStorageLookupResult<TKey, List<TKey>, TEntry>( storage.Storage, from, to );
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
