using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class TsdbEngine
   {
      private IPerformanceStorage _performanceStorage;
      private IVolumeStorage _volumeStorage;
      private IPublishSubscribe _publishSubscribe;

      public TsdbEngine( 
         IPerformanceStorage performanceStorage, 
         IVolumeStorage volumeStorage, 
         IPublishSubscribe publishSubscribe )
      {
         _performanceStorage = performanceStorage;
         _volumeStorage = volumeStorage;
         _publishSubscribe = publishSubscribe;
      }

      public async Task Complete( string id )
      {
         var result = await _performanceStorage.Read( id ).ConfigureAwait( false ); // do we need ReadAndDelete in single transaction??? I think we might
         await _volumeStorage.Write( result.Entries ).ConfigureAwait( false );
         await _performanceStorage.Delete( id ).ConfigureAwait( false );
      }

      public Task Store( IEnumerable<IEntry> items )
      {
         return _performanceStorage.Write( items );
         
         // TODO: Publish
      }

      public async Task<ReadResult<TEntry>> RetrieveAs<TEntry>( string id )
         where TEntry : IEntry
      {
         var performanceTask = _performanceStorage.ReadAs<TEntry>( id );
         var volumeTask = _volumeStorage.ReadAs<TEntry>( id );

         // wait for completion
         await Task.WhenAll( performanceTask, volumeTask ).ConfigureAwait( false );

         // get the results
         var performanceResult = performanceTask.Result;
         var volumeResult = volumeTask.Result;

         // merge
         return performanceResult.MergeWith( volumeResult );
      }

      public async Task<ReadResult<TEntry>> RetrieveAs<TEntry>( string id, DateTime from, DateTime to )
         where TEntry : IEntry
      {
         var performanceTask = _performanceStorage.ReadAs<TEntry>( id, from, to );
         var volumeTask = _volumeStorage.ReadAs<TEntry>( id, from, to );

         // wait for completion
         await Task.WhenAll( performanceTask, volumeTask ).ConfigureAwait( false );

         // get the results
         var performanceResult = performanceTask.Result;
         var volumeResult = volumeTask.Result;

         // merge
         return performanceResult.MergeWith( volumeResult );
      }
   }
}
