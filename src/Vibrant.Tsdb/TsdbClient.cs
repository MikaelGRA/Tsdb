using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbClient<TKey, TEntry> : IStorage<TKey, TEntry>, ISubscribe<TKey, TEntry>
      where TEntry : IEntry<TKey>
   {
      public event EventHandler<TsdbWriteFailureEventArgs<TKey, TEntry>> WriteFailure;
      public event EventHandler<TsdbWriteFailureEventArgs<TKey, TEntry>> TemporaryWriteFailure;

      private TEntry[] _entries = new TEntry[ 0 ];
      private IDynamicStorageSelector<TKey, TEntry> _dynamicStorageSelector;
      private IVolumeStorageSelector<TKey, TEntry> _volumeStorageSelector;
      private IPublishSubscribe<TKey, TEntry> _remotePublishSubscribe;
      private ITemporaryStorage<TKey, TEntry> _temporaryStorage;
      private DefaultPublishSubscribe<TKey, TEntry> _localPublishSubscribe;

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector,
         IPublishSubscribe<TKey, TEntry> remotePublishSubscribe,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
      {
         _dynamicStorageSelector = dynamicStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _remotePublishSubscribe = remotePublishSubscribe;
         _temporaryStorage = temporaryStorage;
         _localPublishSubscribe = new DefaultPublishSubscribe<TKey, TEntry>( false );
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
         : this( dynamicStorageSelector, volumeStorageSelector, null, temporaryStorage )
      {
      }

      public TsdbClient(
         IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector,
         ITemporaryStorage<TKey, TEntry> temporaryStorage )
         : this( dynamicStorageSelector, null, null, temporaryStorage )
      {
      }

      public async Task MoveToVolumeStorage( IEnumerable<TKey> ids, int batchSize )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }

         var tasks = new List<Task>();
         tasks.AddRange( ids.Select( id => MoveToVolumeStorage( id, batchSize ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task MoveToVolumeStorage( IEnumerable<TKey> ids, int batchSize, DateTime to )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }

         var tasks = new List<Task>();
         tasks.AddRange( ids.Select( id => MoveToVolumeStorage( id, batchSize, to ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task MoveToVolumeStorage( TKey id, int batchSize )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }

         var dynamic = _dynamicStorageSelector.GetStorage( id );
         var volume = _volumeStorageSelector.GetStorage( id );

         IContinuationToken token = null;
         do
         {
            var segment = await dynamic.Read( id, batchSize, token ).ConfigureAwait( false );
            await volume.Write( segment.Entries ).ConfigureAwait( false );
            await segment.DeleteAsync().ConfigureAwait( false );
            token = segment.ContinuationToken;
         }
         while( token.HasMore );
      }

      public async Task MoveToVolumeStorage( TKey id, int batchSize, DateTime to )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }

         var dynamic = _dynamicStorageSelector.GetStorage( id );
         var volume = _volumeStorageSelector.GetStorage( id );

         IContinuationToken token = null;
         do
         {
            var segment = await dynamic.Read( id, to, batchSize, token ).ConfigureAwait( false );
            await volume.Write( segment.Entries ).ConfigureAwait( false );
            await segment.DeleteAsync().ConfigureAwait( false );
            token = segment.ContinuationToken;
         }
         while( token.HasMore );
      }

      public async Task MoveFromTemporaryStorage( int batchSize )
      {
         int read = 0;
         do
         {
            // read
            var batch = _temporaryStorage.Read( batchSize );

            // set read count
            read = batch.Entries.Count;

            if( read > 0 )
            {
               // write to volumetric
               var tasks = new List<Task>();
               tasks.AddRange( LookupDynamicStorages( batch.Entries ).Select( c => c.Storage.Write( c.Lookups ) ) );
               await Task.WhenAll( tasks ).ConfigureAwait( false );

               // delete
               batch.Delete();
            }
         }
         while( read != 0 );
      }

      public async Task WriteDirectlyToVolumeStorage( IEnumerable<TEntry> items )
      {
         if( _volumeStorageSelector == null )
         {
            throw new InvalidOperationException( "No volume storage has been provided for this TsdbClient." );
         }

         var tasks = new List<Task>();
         tasks.AddRange( LookupVolumeStorages( items ).Select( c => c.Storage.Write( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         return Write( items, PublicationType.None, Publish.Nowhere, true );
      }

      public Task Write( IEnumerable<TEntry> items, bool useTemporaryStorageOnFailure )
      {
         return Write( items, PublicationType.None, Publish.Nowhere, useTemporaryStorageOnFailure );
      }

      public Task Write( IEnumerable<TEntry> items, PublicationType publicationType )
      {
         return Write( items, publicationType, publicationType != PublicationType.None ? Publish.LocallyAndRemotely : Publish.Nowhere, true );
      }

      public async Task Write( IEnumerable<TEntry> items, PublicationType publicationType, Publish publish, bool useTemporaryStorageOnFailure )
      {
         // ensure we only iterate the original collection once, if it is not a list or array
         if( !( items is IList<TEntry> || items is Array ) )
         {
            items = items.ToList();
         }

         var tasks = new List<Task<IEnumerable<TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( items ).Select( c => WriteToDynamicStorage( c.Storage, c.Lookups, useTemporaryStorageOnFailure ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // Only publish things that were written
         var writtenItems = tasks.SelectMany( x => x.Result );

         if( publish.HasFlag( Publish.Remotely ) )
         {
            if( _remotePublishSubscribe == null )
            {
               throw new InvalidOperationException( "No remote publish subscribe store has been provided for the TsdbClient." );
            }

            await _remotePublishSubscribe.Publish( writtenItems, publicationType ).ConfigureAwait( false );
         }
         if( publish.HasFlag( Publish.Locally ) )
         {
            await _localPublishSubscribe.Publish( writtenItems, publicationType ).ConfigureAwait( false );
         }
      }

      private async Task<IEnumerable<TEntry>> WriteToDynamicStorage( IDynamicStorage<TKey, TEntry> storage, IEnumerable<TEntry> entries, bool useTemporaryStorageOnFailure )
      {
         try
         {
            await storage.Write( entries ).ConfigureAwait( false );
            return entries;
         }
         catch( Exception e1 )
         {
            if( useTemporaryStorageOnFailure )
            {
               try
               {
                  _temporaryStorage.Write( entries );
               }
               catch( Exception e2 )
               {
                  TemporaryWriteFailure?.Invoke( this, new TsdbWriteFailureEventArgs<TKey, TEntry>( entries, e2 ) );
               }
            }

            WriteFailure?.Invoke( this, new TsdbWriteFailureEventArgs<TKey, TEntry>( entries, e1 ) );
            return _entries;
         }
      }

      public async Task Delete( IEnumerable<TKey> ids )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task Delete( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadLatest( IEnumerable<TKey> ids )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.ReadLatest( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // at this point we need to check if we have a measurement for each id. We might not becuase we only looked in dynamic store
         var result = tasks.Select( x => x.Result ).Combine();

         if( _volumeStorageSelector != null )
         {
            // find missing ids
            List<TKey> missingIds = new List<TKey>();
            foreach( var id in ids )
            {
               var resultForId = result.FindResult( id );
               if( resultForId.Entries.Count == 0 )
               {
                  missingIds.Add( id );
               }
            }

            // if missing ids, then we look at volume storage
            if( missingIds.Count > 0 )
            {
               tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
               tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadLatest( c.Lookups ) ) );
               await Task.WhenAll( tasks ).ConfigureAwait( false );

               var intiallyMissingResult = tasks.Select( x => x.Result ).Combine();
               intiallyMissingResult.MergeInto( result );
            }
         }

         return result;
      }

      public async Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Read( c.Lookups, sort ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups, sort ) ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TKey, TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to, sort ) ) );
         if( _volumeStorageSelector != null )
         {
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to, sort ) ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public Task<Func<Task>> Subscribe( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         if( _remotePublishSubscribe == null )
         {
            throw new InvalidOperationException( "No remote publish subscribe store has been provided for the TsdbClient." );
         }

         return _remotePublishSubscribe.Subscribe( ids, subscribe, callback );
      }

      public Task<Func<Task>> SubscribeToAll( SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         if( _remotePublishSubscribe == null )
         {
            throw new InvalidOperationException( "No remote publish subscribe store has been provided for the TsdbClient." );
         }

         return _remotePublishSubscribe.SubscribeToAll( subscribe, callback );
      }

      public Task<Func<Task>> SubscribeLocally( IEnumerable<TKey> ids, SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         return _localPublishSubscribe.Subscribe( ids, subscribe, callback );
      }

      public Task<Func<Task>> SubscribeToAllLocally( SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         return _localPublishSubscribe.SubscribeToAll( subscribe, callback );
      }

      #region Lookup

      private IEnumerable<VolumeStorageLookupResult<TKey, TEntry, TEntry>> LookupVolumeStorages( IEnumerable<TEntry> entries )
      {
         var result = new Dictionary<IStorage<TKey, TEntry>, VolumeStorageLookupResult<TKey, TEntry, TEntry>>();

         foreach( var entry in entries )
         {
            var storageForId = _volumeStorageSelector.GetStorage( entry.GetKey() );

            VolumeStorageLookupResult<TKey, TEntry, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new VolumeStorageLookupResult<TKey, TEntry, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( entry );
         }

         return result.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<TKey, TEntry, TEntry>> LookupDynamicStorages( IEnumerable<TEntry> entries )
      {
         var result = new Dictionary<IStorage<TKey, TEntry>, DynamicStorageLookupResult<TKey, TEntry, TEntry>>();

         foreach( var entry in entries )
         {
            var storageForId = _dynamicStorageSelector.GetStorage( entry.GetKey() );

            DynamicStorageLookupResult<TKey, TEntry, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new DynamicStorageLookupResult<TKey, TEntry, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( entry );
         }

         return result.Values;
      }

      private IEnumerable<VolumeStorageLookupResult<TKey, TKey, TEntry>> LookupVolumeStorages( IEnumerable<TKey> ids )
      {
         var result = new Dictionary<IStorage<TKey, TEntry>, VolumeStorageLookupResult<TKey, TKey, TEntry>>();

         foreach( var id in ids )
         {
            var storageForId = _volumeStorageSelector.GetStorage( id );

            VolumeStorageLookupResult<TKey, TKey, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new VolumeStorageLookupResult<TKey, TKey, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( id );
         }

         return result.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<TKey, TKey, TEntry>> LookupDynamicStorages( IEnumerable<TKey> ids )
      {
         var result = new Dictionary<IStorage<TKey, TEntry>, DynamicStorageLookupResult<TKey, TKey, TEntry>>();

         foreach( var id in ids )
         {
            var storageForId = _dynamicStorageSelector.GetStorage( id );

            DynamicStorageLookupResult<TKey, TKey, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new DynamicStorageLookupResult<TKey, TKey, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( id );
         }

         return result.Values;
      }

      #endregion
   }
}
