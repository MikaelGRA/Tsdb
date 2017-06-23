using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb.Client
{
   public class AggregationTsdbClient<TKey, TEntry, TMeasureType>
      where TEntry : IEntry
      where TMeasureType : IMeasureType
   {
      private readonly IAggregateFunctions<TEntry, TMeasureType> _aggregateFunctions;
      private readonly ITypedKeyStorage<TKey, TMeasureType> _typedKeyStorage;
      private readonly IDynamicStorageSelector<TKey, TEntry> _dynamicStorageSelector;
      private readonly IVolumeStorageSelector<TKey, TEntry> _volumeStorageSelector;
      private readonly ITsdbLogger _logger;

      public AggregationTsdbClient( IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector, IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector, ITypedKeyStorage<TKey, TMeasureType> typedKeyStorage, IAggregateFunctions<TEntry, TMeasureType> aggregateFunctions, ITsdbLogger logger )
      {
         _aggregateFunctions = aggregateFunctions;
         _typedKeyStorage = typedKeyStorage;
         _dynamicStorageSelector = dynamicStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _logger = logger;
      }

      public AggregationTsdbClient( IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector, ITypedKeyStorage<TKey, TMeasureType> typedKeyStorage, IAggregateFunctions<TEntry, TMeasureType> aggregateFunctions, ITsdbLogger logger )
         : this( dynamicStorageSelector, null, typedKeyStorage, aggregateFunctions, logger )
      {
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( string measureTypeName, IEnumerable<KeyValuePair<string, string>> requiredTags, IEnumerable<string> groupByTags, GroupMethod groupMethod, Sort sort = Sort.Descending )
      {
         var groupByTagsList = groupByTags.ToList(); // only iterate once
         var requiredTagsDictionary = requiredTags.ToDictionary( x => x.Key, x => x.Value );

         // IF specific store does not support tags? But how do I know before having the keys?
         var typedKeys = await _typedKeyStorage.GetTaggedKeysAsync( measureTypeName, requiredTagsDictionary ).ConfigureAwait( false );

         // get results per storage
         var tasks = LookupDynamicStorages( typedKeys ).Select( c => ReadThroughTagsAsync( c.Storage, c.Lookups, measureTypeName, requiredTagsDictionary, groupByTagsList, groupMethod, sort ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         var results = tasks.Select( x => x.Result ).ToList();
         if( results.Count == 1 )
         {
            return results[ 0 ];
         }
         else
         {
            // TODO: merge all together
            return null;
         }
      }

      private async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadThroughTagsAsync( IStorage<TKey, TEntry> storage, IEnumerable<ITypedKey<TKey, TMeasureType>> typedKeys, string measureTypeName, Dictionary<string, string> requiredTags, List<string> groupByTags, GroupMethod groupMethod, Sort sort )
      {
         var typedStorage = storage as ITypedStorage<TKey, TEntry, TMeasureType>;
         if( typedStorage != null )
         {
            return await typedStorage.ReadGroupsAsync( measureTypeName, requiredTags, groupByTags, groupMethod, sort );
         }
         else
         {
            // create lookup dictionary for keys to typed keys
            var lookups = typedKeys.ToDictionary( x => x.Key );
            var keys = lookups.Keys.ToList();
            var type = lookups.First().Value.GetMeasureType();
            var aggregationMethod = GetAggregationFunction( groupMethod );

            // get 'traditional results'
            var result = await storage.ReadAsync( keys, sort );

            // change into result with tagged keys
            var typedResults = result.WithTags( lookups );

            // perform grouping
            var groupedResults = new Dictionary<TagCollection, List<TypedReadResult<TKey, TEntry, TMeasureType>>>();
            foreach( var taggedResult in typedResults )
            {
               // construct key from dictionary
               var dict = new Dictionary<string, string>();
               foreach( var name in groupByTags )
               {
                  var value = taggedResult.TypedKey.GetTagValue( name );
                  dict.Add( name, value );

                  // FIXME: value MIGHT BE NULL??????????
               }
               var key = new TagCollection( dict );

               // add result to correct group based on created key
               List<TypedReadResult<TKey, TEntry, TMeasureType>> existingList;
               if( !groupedResults.TryGetValue( key, out existingList ) )
               {
                  existingList = new List<TypedReadResult<TKey, TEntry, TMeasureType>>();
                  groupedResults.Add( key, existingList );
               }
               existingList.Add( taggedResult );
            }

            // construct final result from previously grouped results
            var finalResults = new Dictionary<TagCollection, TaggedReadResult<TEntry, TMeasureType>>();
            foreach( var groupedResult in groupedResults )
            {
               var newCollection = MergeSort.Sort(
                  collections: groupedResult.Value.Select( x => x.Entries ),
                  comparer: EntryComparer.GetComparer<TKey, TEntry>( sort ),
                  resolveConflict: x => aggregationMethod( x, type ) );

               //  need tag information RIGHT HERE
               var tagCollection = groupedResult.Key;
               finalResults.Add( tagCollection, new TaggedReadResult<TEntry, TMeasureType>( type, tagCollection, sort, newCollection ) );
            }
            var finalResult = new MultiTaggedReadResult<TEntry, TMeasureType>( finalResults );

            return finalResult;
         }
      }

      private Func<IEnumerable<TEntry>, TMeasureType, TEntry> GetAggregationFunction( GroupMethod method )
      {
         switch( method )
         {
            case GroupMethod.Average:
               return _aggregateFunctions.Average;
            case GroupMethod.Sum:
               return _aggregateFunctions.Sum;
            case GroupMethod.Min:
               return _aggregateFunctions.Min;
            case GroupMethod.Max:
               return _aggregateFunctions.Max;
            default:
               throw new ArgumentException( "Invalid group method specified.", nameof( method ) );
         }
      }

      private IEnumerable<DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>> LookupDynamicStorages( IEnumerable<ITypedKey<TKey, TMeasureType>> taggedIds )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>, DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>>();

         foreach( var id in taggedIds )
         {
            var storages = _dynamicStorageSelector.GetStorage( id.Key, null, null );
            foreach( var storage in storages )
            {
               DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  existingStorage = new DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>( storage.Storage );
                  existingStorage.Lookups = new List<ITypedKey<TKey, TMeasureType>>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>> LookupDynamicStorages( IEnumerable<ITypedKey<TKey, TMeasureType>> taggedIds, DateTime from, DateTime to )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>, DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>>();

         foreach( var id in taggedIds )
         {
            var storages = _dynamicStorageSelector.GetStorage( id.Key, null, null );
            foreach( var storage in storages )
            {
               DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  existingStorage = new DynamicStorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>( storage.Storage, from, to );
                  existingStorage.Lookups = new List<ITypedKey<TKey, TMeasureType>>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }
   }
}
