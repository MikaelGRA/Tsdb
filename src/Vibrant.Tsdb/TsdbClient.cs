using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbClient<TEntry> : IStorage<TEntry>, ISubscribe<TEntry>
      where TEntry : IEntry
   {
      public event EventHandler<TsdbWriteFailureEventArgs<TEntry>> WriteFailure;
      public event EventHandler<TsdbWriteFailureEventArgs<TEntry>> TemporaryWriteFailure;

      private TEntry[] _entries = new TEntry[ 0 ];
      private IDynamicStorageSelector<TEntry> _dynamicStorageSelector;
      private IVolumeStorageSelector<TEntry> _volumeStorageSelector;
      private IPublishSubscribe<TEntry> _remotePublishSubscribe;
      private ITemporaryStorage<TEntry> _temporaryStorage;
      private DefaultPublishSubscribe<TEntry> _localPublishSubscribe;

      public TsdbClient(
         IDynamicStorageSelector<TEntry> dynamicStorageSelector,
         IVolumeStorageSelector<TEntry> volumeStorageSelector,
         IPublishSubscribe<TEntry> remotePublishSubscribe,
         ITemporaryStorage<TEntry> temporaryStorage )
      {
         _dynamicStorageSelector = dynamicStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _remotePublishSubscribe = remotePublishSubscribe;
         _temporaryStorage = temporaryStorage;
         _localPublishSubscribe = new DefaultPublishSubscribe<TEntry>( false );
      }

      public async Task MoveToVolumeStorage( IEnumerable<string> ids )
      {
         var tasks = new List<Task>();
         tasks.AddRange( ids.Select( id => MoveToVolumeStorage( id ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task MoveToVolumeStorage( IEnumerable<string> ids, DateTime to )
      {
         var tasks = new List<Task>();
         tasks.AddRange( ids.Select( id => MoveToVolumeStorage( id, to ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }
      
      public async Task MoveToVolumeStorage( string id )
      {
         var dynamic = _dynamicStorageSelector.GetStorage( id );
         var volume = _volumeStorageSelector.GetStorage( id );

         IContinuationToken token = null;
         do
         {
            var segment = await dynamic.Read( id, 10000, token ).ConfigureAwait( false );
            await volume.Write( segment.Entries ).ConfigureAwait( false );
            await segment.DeleteAsync().ConfigureAwait( false );
            token = segment.ContinuationToken;
         }
         while( token.HasMore );
      }

      public async Task MoveToVolumeStorage( string id, DateTime to )
      {
         var dynamic = _dynamicStorageSelector.GetStorage( id );
         var volume = _volumeStorageSelector.GetStorage( id );

         IContinuationToken token = null;
         do
         {
            var segment = await dynamic.Read( id, to, 10000, token ).ConfigureAwait( false );
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

            // write to volumetric
            var tasks = new List<Task>();
            tasks.AddRange( LookupDynamicStorages( batch.Entries ).Select( c => c.Storage.Write( c.Lookups ) ) );
            await Task.WhenAll( tasks ).ConfigureAwait( false );

            // delete
            batch.Delete();

            // set read count
            read = batch.Entries.Count;
         }
         while( read != 0 );
      }

      public async Task WriteDirectlyToVolumeStorage( IEnumerable<TEntry> items )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupVolumeStorages( items ).Select( c => c.Storage.Write( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         return Write( items, PublicationType.None, Publish.Nowhere, false );
      }

      public Task Write( IEnumerable<TEntry> items, bool useTemporaryStorageOnFailure )
      {
         return Write( items, PublicationType.None, Publish.Nowhere, useTemporaryStorageOnFailure );
      }

      public Task Write( IEnumerable<TEntry> items, PublicationType publicationType )
      {
         return Write( items, publicationType, publicationType != PublicationType.None ? Publish.LocallyAndRemotely : Publish.Nowhere, false );
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
            await _remotePublishSubscribe.Publish( writtenItems, publicationType ).ConfigureAwait( false );
         }
         if( publish.HasFlag( Publish.Locally ) )
         {
            await _localPublishSubscribe.Publish( writtenItems, publicationType ).ConfigureAwait( false );
         }
      }

      private async Task<IEnumerable<TEntry>> WriteToDynamicStorage( IDynamicStorage<TEntry> storage, IEnumerable<TEntry> entries, bool useTemporaryStorageOnFailure )
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
                  TemporaryWriteFailure?.Invoke( this, new TsdbWriteFailureEventArgs<TEntry>( entries, e2 ) );
               }
            }

            WriteFailure?.Invoke( this, new TsdbWriteFailureEventArgs<TEntry>( entries, e1 ) );
            return _entries;
         }
      }

      public async Task Delete( IEnumerable<string> ids )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.ReadLatest( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // at this point we need to check if we have a measurement for each id. We might not becuase we only looked in dynamic store
         var result = tasks.Select( x => x.Result ).Combine();

         // find missing ids
         List<string> missingIds = new List<string>();
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
            tasks = new List<Task<MultiReadResult<TEntry>>>();
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadLatest( c.Lookups ) ) );
            await Task.WhenAll( tasks ).ConfigureAwait( false );

            var intiallyMissingResult = tasks.Select( x => x.Result ).Combine();
            intiallyMissingResult.MergeInto( result );
         }

         return result;
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Read( c.Lookups, sort ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups, sort ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
         tasks.AddRange( LookupDynamicStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to, sort ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to, sort ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public Task<Func<Task>> Subscribe( IEnumerable<string> ids, SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         return _remotePublishSubscribe.Subscribe( ids, subscribe, callback );
      }

      public Task<Func<Task>> SubscribeToAll( SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         return _remotePublishSubscribe.SubscribeToAll( subscribe, callback );
      }

      public Task<Func<Task>> SubscribeLocally( IEnumerable<string> ids, SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         return _localPublishSubscribe.Subscribe( ids, subscribe, callback );
      }

      public Task<Func<Task>> SubscribeToAllLocally( SubscriptionType subscribe, Action<List<TEntry>> callback )
      {
         return _localPublishSubscribe.SubscribeToAll( subscribe, callback );
      }

      #region Lookup

      private IEnumerable<VolumeStorageLookupResult<TEntry, TEntry>> LookupVolumeStorages( IEnumerable<TEntry> entries )
      {
         var result = new Dictionary<IStorage<TEntry>, VolumeStorageLookupResult<TEntry, TEntry>>();

         foreach( var entry in entries )
         {
            var storageForId = _volumeStorageSelector.GetStorage( entry.GetId() );

            VolumeStorageLookupResult<TEntry, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new VolumeStorageLookupResult<TEntry, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( entry );
         }

         return result.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<TEntry, TEntry>> LookupDynamicStorages( IEnumerable<TEntry> entries )
      {
         var result = new Dictionary<IStorage<TEntry>, DynamicStorageLookupResult<TEntry, TEntry>>();

         foreach( var entry in entries )
         {
            var storageForId = _dynamicStorageSelector.GetStorage( entry.GetId() );

            DynamicStorageLookupResult<TEntry, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new DynamicStorageLookupResult<TEntry, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( entry );
         }

         return result.Values;
      }

      private IEnumerable<VolumeStorageLookupResult<string, TEntry>> LookupVolumeStorages( IEnumerable<string> ids )
      {
         var result = new Dictionary<IStorage<TEntry>, VolumeStorageLookupResult<string, TEntry>>();

         foreach( var id in ids )
         {
            var storageForId = _volumeStorageSelector.GetStorage( id );

            VolumeStorageLookupResult<string, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new VolumeStorageLookupResult<string, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( id );
         }

         return result.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<string, TEntry>> LookupDynamicStorages( IEnumerable<string> ids )
      {
         var result = new Dictionary<IStorage<TEntry>, DynamicStorageLookupResult<string, TEntry>>();

         foreach( var id in ids )
         {
            var storageForId = _dynamicStorageSelector.GetStorage( id );

            DynamicStorageLookupResult<string, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new DynamicStorageLookupResult<string, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( id );
         }

         return result.Values;
      }

      #endregion
   }
}
