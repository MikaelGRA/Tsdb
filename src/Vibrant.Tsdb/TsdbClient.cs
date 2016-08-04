using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbClient<TEntry> : IStorage<TEntry>, ISubscribe<TEntry>
      where TEntry : IEntry
   {
      private IPerformanceStorageSelector<TEntry> _performanceStorageSelector;
      private IVolumeStorageSelector<TEntry> _volumeStorageSelector;
      private IPublishSubscribe<TEntry> _publishSubscribe;

      public TsdbClient(
         IPerformanceStorageSelector<TEntry> performanceStorageSelector,
         IVolumeStorageSelector<TEntry> volumeStorageSelector,
         IPublishSubscribe<TEntry> publishSubscribe )
      {
         _performanceStorageSelector = performanceStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _publishSubscribe = publishSubscribe;
      }

      public async Task<int> MoveToVolumeStorage( IEnumerable<string> ids )
      {
         // read from performance storage
         var readTasks = new List<Task<MultiReadResult<TEntry>>>();
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
         var readTasks = new List<Task<MultiReadResult<TEntry>>>();
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

      public async Task WriteDirectlyToVolumeStorage( IEnumerable<TEntry> items )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupVolumeStorages( items ).Select( c => c.Storage.Write( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         return Write( items, Publish.None );
      }

      public async Task Write( IEnumerable<TEntry> items, Publish publish )
      {
         var tasks = new List<Task>();
         tasks.AddRange( LookupPerformanceStorages( items ).Select( c => c.Storage.Write( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         switch( publish )
         {
            case Publish.None:
               break;
            case Publish.Latest:
               await _publishSubscribe.Publish( FindLatestForEachId( items ) ).ConfigureAwait( false );
               break;
            case Publish.All:
               await _publishSubscribe.Publish( items ).ConfigureAwait( false );
               break;
            default:
               throw new ArgumentException( "publish" );
         }
      }

      public async Task<int> Delete( IEnumerable<string> ids )
      {
         var tasks = new List<Task<int>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Sum( x => x.Result );
      }

      public async Task<int> Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task<int>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Delete( c.Lookups, from, to ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Sum( x => x.Result );
      }

      //public async Task<MultiReadResult<TEntry>> ReadLatestAs<TEntry>( IEnumerable<string> ids )
      //   where TEntry : IEntry
      //{
      //   var tasks = new List<Task<MultiReadResult<TEntry>>>();
      //   tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.ReadLatestAs<TEntry>( c.Lookups ) ) );
      //   await Task.WhenAll( tasks ).ConfigureAwait( false );

      //   // at this point we need to check if we have a measurement for each id. We might not becuase we only looked in performance store
      //   var result = tasks.Select( x => x.Result ).Combine();

      //   // find missing ids
      //   List<string> missingIds = new List<string>();
      //   foreach( var id in ids )
      //   {
      //      var resultForId = result.FindResult( id );
      //      if( resultForId.Entries.Count == 0 )
      //      {
      //         missingIds.Add( id );
      //      }
      //   }

      //   // if missing ids, then we look at volume storage
      //   if( missingIds.Count > 0 )
      //   {
      //      tasks = new List<Task<MultiReadResult<TEntry>>>();
      //      tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.ReadLatestAs<TEntry>( c.Lookups ) ) );
      //      await Task.WhenAll( tasks ).ConfigureAwait( false );

      //      var intiallyMissingResult = tasks.Select( x => x.Result ).Combine();
      //      intiallyMissingResult.MergeInto( result );
      //   }

      //   return result;
      //}

      public async Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
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
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Read( c.Lookups, sort ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups, sort ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<MultiReadResult<TEntry>>>();
         tasks.AddRange( LookupPerformanceStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to, sort ) ) );
         tasks.AddRange( LookupVolumeStorages( ids ).Select( c => c.Storage.Read( c.Lookups, from, to, sort ) ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public Task<Func<Task>> Subscribe( IEnumerable<string> ids, Action<List<TEntry>> callback )
      {
         return _publishSubscribe.Subscribe( ids, callback );
      }

      public Task<Func<Task>> SubscribeToAll( Action<List<TEntry>> callback )
      {
         return _publishSubscribe.SubscribeToAll( callback );
      }

      #region Lookup

      private IEnumerable<TEntry> FindLatestForEachId( IEnumerable<TEntry> entries )
      {
         var foundEntries = new Dictionary<string, TEntry>();
         foreach( var entry in entries )
         {
            var id = entry.GetId();

            TEntry existingEntry;
            if( !foundEntries.TryGetValue( id, out existingEntry ) )
            {
               foundEntries.Add( id, entry );
            }
            else
            {
               if( entry.GetTimestamp() > existingEntry.GetTimestamp() )
               {
                  foundEntries[ id ] = entry;
               }
            }
         }
         return foundEntries.Values;
      }

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

      private IEnumerable<PerformanceStorageLookupResult<TEntry, TEntry>> LookupPerformanceStorages( IEnumerable<TEntry> entries )
      {
         var result = new Dictionary<IStorage<TEntry>, PerformanceStorageLookupResult<TEntry, TEntry>>();

         foreach( var entry in entries )
         {
            var storageForId = _performanceStorageSelector.GetStorage( entry.GetId() );

            PerformanceStorageLookupResult<TEntry, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new PerformanceStorageLookupResult<TEntry, TEntry>( storageForId );
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

      private IEnumerable<PerformanceStorageLookupResult<string, TEntry>> LookupPerformanceStorages( IEnumerable<string> ids )
      {
         var result = new Dictionary<IStorage<TEntry>, PerformanceStorageLookupResult<string, TEntry>>();

         foreach( var id in ids )
         {
            var storageForId = _performanceStorageSelector.GetStorage( id );

            PerformanceStorageLookupResult<string, TEntry> existingStorage;
            if( !result.TryGetValue( storageForId, out existingStorage ) )
            {
               existingStorage = new PerformanceStorageLookupResult<string, TEntry>( storageForId );
               result.Add( storageForId, existingStorage );
            }

            existingStorage.Lookups.Add( id );
         }

         return result.Values;
      }

      #endregion
   }
}
