using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbEngine
   {
      private IPerformanceStorageSelector _performanceStorageSelector;
      private IVolumeStorageSelector _volumeStorageSelector;
      private IPublishSubscribe _publishSubscribe;

      public TsdbEngine(
         IPerformanceStorageSelector performanceStorageSelector,
         IVolumeStorageSelector volumeStorageSelector,
         IPublishSubscribe publishSubscribe )
      {
         _performanceStorageSelector = performanceStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _publishSubscribe = publishSubscribe;
      }

      //public async Task Complete( string id )
      //{
      //   var result = await _performanceStorage.Read( id ).ConfigureAwait( false ); // do we need ReadAndDelete in single transaction??? I think we might
      //   await _volumeStorage.Write( result.Entries ).ConfigureAwait( false );
      //   await _performanceStorage.Delete( id ).ConfigureAwait( false );
      //}

      public async Task Write( IEnumerable<IEntry> items )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupPerformanceStorages( items ).Select( c => c.Storage.Write( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // TODO: Publish
      }

      public async Task Delete( IEnumerable<string> ids )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task<MultiReadResult<TEntry>> ReadAs<TEntry>( IEnumerable<string> ids )
         where TEntry : IEntry
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.ReadAs<TEntry>( c.Lookups ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadAs<TEntry>( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TEntry>> ReadAs<TEntry>( IEnumerable<string> ids, DateTime from, DateTime to )
         where TEntry : IEntry
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.ReadAs<TEntry>( c.Lookups, from, to ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadAs<TEntry>( c.Lookups, from, to ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids )
      {
         var tasks = new List<Task<MultiReadResult<IEntry>>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Read( c.Lookups ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<IEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task<MultiReadResult<IEntry>>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      #region Lookup

      private IEnumerable<VolumeStorageLookupResult<IEntry>> LookupVolumeStorages( IEnumerable<IEntry> entries )
      {
         var result = new Dictionary<IStorage, VolumeStorageLookupResult<IEntry>>();

         foreach( var entry in entries )
         {
            var storageForId = _volumeStorageSelector.GetStorage( entry.GetId() );

            VolumeStorageLookupResult<IEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new VolumeStorageLookupResult<IEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( entry );
         }

         return result.Values;
      }

      private IEnumerable<PerformanceStorageLookupResult<IEntry>> LookupPerformanceStorages( IEnumerable<IEntry> entries )
      {
         var result = new Dictionary<IStorage, PerformanceStorageLookupResult<IEntry>>();

         foreach( var entry in entries )
         {
            var storageForId = _performanceStorageSelector.GetStorage( entry.GetId() );

            PerformanceStorageLookupResult<IEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new PerformanceStorageLookupResult<IEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( entry );
         }

         return result.Values;
      }

      private IEnumerable<VolumeStorageLookupResult<string>> LookupVolumeStorages( IEnumerable<string> ids )
      {
         var result = new Dictionary<IStorage, VolumeStorageLookupResult<string>>();

         foreach( var id in ids )
         {
            var storageForId = _volumeStorageSelector.GetStorage( id );

            VolumeStorageLookupResult<string> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new VolumeStorageLookupResult<string>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( id );
         }

         return result.Values;
      }

      private IEnumerable<PerformanceStorageLookupResult<string>> LookupPerformanceStorages( IEnumerable<string> ids )
      {
         var result = new Dictionary<IStorage, PerformanceStorageLookupResult<string>>();

         foreach( var id in ids )
         {
            var storageForId = _performanceStorageSelector.GetStorage( id );

            PerformanceStorageLookupResult<string> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new PerformanceStorageLookupResult<string>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( id );
         }

         return result.Values;
      }

      #endregion
   }
}
