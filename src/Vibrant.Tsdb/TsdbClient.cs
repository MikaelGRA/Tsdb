using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbClient
   {
      private IPerformanceStorageSelector _performanceStorageSelector;
      private IVolumeStorageSelector _volumeStorageSelector;
      private IPublishSubscribe _publishSubscribe;

      public TsdbClient(
         IPerformanceStorageSelector performanceStorageSelector,
         IVolumeStorageSelector volumeStorageSelector,
         IPublishSubscribe publishSubscribe )
      {
         _performanceStorageSelector = performanceStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _publishSubscribe = publishSubscribe;
      }

      public async Task<int> MoveToVolumeStorage( IEnumerable<string> ids )
      {
         // read from performance storage
         var readTasks = new List<Task<MultiReadResult<IEntry>>>();
         readTasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Read( c.Lookups ) ) );
         await Task.WhenAll( readTasks ).ConfigureAwait( false );

         // write to volume storage
         var entries = readTasks.SelectMany( x => x.Result ).SelectMany( x => x.Entries );
         await WriteDirectlyToVolumeStorage( entries ).ConfigureAwait( false );

         // delete from performance storage
         var deleteTasks = new List<Task<int>>();
         deleteTasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         await Task.WhenAll( deleteTasks ).ConfigureAwait( false );

         // return amount deleted (also moved)
         return deleteTasks.Sum( x => x.Result );
      }

      public async Task<int> MoveToVolumeStorage( IEnumerable<string> ids, DateTime to )
      {
         // read from performance storage
         var readTasks = new List<Task<MultiReadResult<IEntry>>>();
         readTasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Read( c.Lookups, to ) ) );
         await Task.WhenAll( readTasks ).ConfigureAwait( false );

         // write to volume storage
         var entries = readTasks.SelectMany( x => x.Result ).SelectMany( x => x.Entries );
         await WriteDirectlyToVolumeStorage( entries ).ConfigureAwait( false );

         // delete from performance storage
         var deleteTasks = new List<Task<int>>();
         deleteTasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, to ) ) );
         await Task.WhenAll( deleteTasks ).ConfigureAwait( false );

         // return amount deleted (also moved)
         return deleteTasks.Sum( x => x.Result );
      }

      public async Task WriteDirectlyToVolumeStorage( IEnumerable<IEntry> items )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupVolumeStorages( items ).Select( c => c.Storage.Write( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task Write( IEnumerable<IEntry> items )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupPerformanceStorages( items ).Select( c => c.Storage.Write( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // QUESTION: Should we publish all??? Or just latest???
         await _publishSubscribe.Publish( items ).ConfigureAwait( false );
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

      public async Task<MultiReadResult<TEntry>> ReadLatestAs<TEntry>( IEnumerable<string> ids )
         where TEntry : IEntry
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.ReadLatestAs<TEntry>( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // at this point we need to check if we have a measurement for each id. We might not becuase we only looked in performance store
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
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadLatestAs<TEntry>( c.Lookups ) ) );
            await Task.WhenAll( tasks ).ConfigureAwait( false );

            var intiallyMissingResult = tasks.Select( x => x.Result ).Combine();
            intiallyMissingResult.MergeInto( result );
         }

         return result;
      }

      public async Task<MultiReadResult<IEntry>> ReadLatest( IEnumerable<string> ids )
      {
         var tasks = new List<Task<MultiReadResult<IEntry>>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.ReadLatest( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // at this point we need to check if we have a measurement for each id. We might not becuase we only looked in performance store
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
            tasks = new List<Task<MultiReadResult<IEntry>>>();
            tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadLatest( c.Lookups ) ) );
            await Task.WhenAll( tasks ).ConfigureAwait( false );

            var intiallyMissingResult = tasks.Select( x => x.Result ).Combine();
            intiallyMissingResult.MergeInto( result );
         }

         return result;
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
