﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb.Client
{
   public class AggregationTsdbClient<TKey, TEntry, TMeasureType>
      where TEntry : IAggregatableEntry, new()
      where TMeasureType : IMeasureType
   {
      private readonly ITypedKeyStorage<TKey, TMeasureType> _typedKeyStorage;
      private readonly IDynamicStorageSelector<TKey, TEntry> _dynamicStorageSelector;
      private readonly IVolumeStorageSelector<TKey, TEntry> _volumeStorageSelector;
      private readonly ITsdbLogger _logger;
      private readonly Dictionary<AggregationExpressionKey, Func<IEnumerable<TEntry>, IFieldInfo[], TEntry>> _cachedExpressions;

      public AggregationTsdbClient( IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector, IVolumeStorageSelector<TKey, TEntry> volumeStorageSelector, ITypedKeyStorage<TKey, TMeasureType> typedKeyStorage, ITsdbLogger logger )
      {
         _typedKeyStorage = typedKeyStorage;
         _dynamicStorageSelector = dynamicStorageSelector;
         _volumeStorageSelector = volumeStorageSelector;
         _logger = logger;

         _cachedExpressions = new Dictionary<AggregationExpressionKey, Func<IEnumerable<TEntry>, IFieldInfo[], TEntry>>();
      }

      public AggregationTsdbClient( IDynamicStorageSelector<TKey, TEntry> dynamicStorageSelector, ITypedKeyStorage<TKey, TMeasureType> typedKeyStorage, ITsdbLogger logger )
         : this( dynamicStorageSelector, null, typedKeyStorage, logger )
      {
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync( 
         string measureTypeName, 
         IEnumerable<KeyValuePair<string, string>> requiredTags, 
         IEnumerable<string> groupByTags, 
         GroupMethod groupMethod, 
         Sort sort = Sort.Descending )
      {
         var sortedGroupByTagsList = groupByTags.ToList(); // only iterate once
         sortedGroupByTagsList.Sort( StringComparer.Ordinal );
         var requiredTagsDictionary = requiredTags.ToDictionary( x => x.Key, x => x.Value );

         // IF specific store does not support tags? But how do I know before having the keys?
         var typedKeys = await _typedKeyStorage.GetTaggedKeysAsync( measureTypeName, requiredTagsDictionary ).ConfigureAwait( false );

         // get results per storage
         var tasks = LookupDynamicStorages( typedKeys ).Select( c => ReadThroughTagsAsync( c.Storage, c.Lookups, measureTypeName, requiredTagsDictionary, sortedGroupByTagsList, groupMethod, sort ) );
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // get list of results
         var results = tasks.Select( x => x.Result ).ToList();

         // perform final merging
         return MergeTaggedResults( groupMethod, sort, results );
      }

      private async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadThroughTagsAsync( 
         IStorage<TKey, TEntry> storage, 
         IEnumerable<ITypedKey<TKey, TMeasureType>> typedKeys, 
         string measureTypeName, 
         Dictionary<string, string> requiredTags, 
         List<string> sortedGroupByTagsList, 
         GroupMethod groupMethod, 
         Sort sort )
      {
         var typedStorage = storage as ITypedStorage<TKey, TEntry, TMeasureType>;
         if( typedStorage != null )
         {
            return await typedStorage.ReadGroupsAsync( measureTypeName, requiredTags, sortedGroupByTagsList, groupMethod, sort ).ConfigureAwait( false );
         }
         else
         {
            // create lookup dictionary for keys to typed keys
            var lookups = typedKeys.ToDictionary( x => x.Key );
            var keys = lookups.Keys.ToList();
            var measureType = lookups.First().Value.GetMeasureType();
            var fields = measureType.GetFields().ToArray();

            // get 'traditional results'
            var result = await storage.ReadAsync( keys, sort ).ConfigureAwait( false );

            // change into result with tagged keys
            var typedResults = result.WithTags( lookups );

            // perform grouping (GroupByTags())
            var groupedResults = GroupByTags( sortedGroupByTagsList, typedResults );

            // construct final result from previously grouped results
            return MergeTypedResults( measureType, fields, groupMethod, sort, groupedResults );
         }
      }

      private MultiTaggedReadResult<TEntry, TMeasureType> MergeTaggedResults( 
         GroupMethod groupMethod,
         Sort sort,
         List<MultiTaggedReadResult<TEntry, TMeasureType>> results )
      {
         if( results.Count == 1 )
         {
            return results[ 0 ];
         }
         else
         {
            // figure out which to merge together
            var finalResultDictionary = new Dictionary<TagCollection, TaggedReadResult<TEntry, TMeasureType>>();

            // foreach tag combination!
            var groupings = new Dictionary<TagCollection, List<TaggedReadResult<TEntry, TMeasureType>>>();
            TMeasureType measureType = default( TMeasureType );
            bool anyResults = false;

            foreach( var multiTaggedReadResult in results )
            {
               foreach( var taggedReadResult in multiTaggedReadResult )
               {
                  measureType = taggedReadResult.MeasureType;
                  anyResults = true;

                  List<TaggedReadResult<TEntry, TMeasureType>> existingList;
                  if( !groupings.TryGetValue( taggedReadResult.GroupedTags, out existingList ) )
                  {
                     existingList = new List<TaggedReadResult<TEntry, TMeasureType>>();
                     groupings.Add( taggedReadResult.GroupedTags, existingList );
                  }
                  existingList.Add( taggedReadResult );
               }
            }

            if( anyResults )
            {
               // need fields and aggregate method
               var fields = measureType.GetFields().ToArray();
               return Merge( measureType, fields, groupMethod, sort, groupings );
            }
            else
            {
               // hmmm? How do we do anything here?? We need a measure type!
               return new MultiTaggedReadResult<TEntry, TMeasureType>();
            }
         }
      }

      private Dictionary<TagCollection, List<TypedReadResult<TKey, TEntry, TMeasureType>>> GroupByTags( 
         List<string> sortedGroupByTagsList,
         MultiTypedReadResult<TKey, TEntry, TMeasureType> typedResults)
      {
         var groupedResults = new Dictionary<TagCollection, List<TypedReadResult<TKey, TEntry, TMeasureType>>>();
         foreach( var taggedResult in typedResults )
         {
            // construct key from dictionary
            var dict = new Dictionary<string, string>();
            foreach( var name in sortedGroupByTagsList )
            {
               var value = taggedResult.TypedKey.GetTagValue( name );
               dict.Add( name, value );
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

         return groupedResults;
      }

      private MultiTaggedReadResult<TEntry, TMeasureType> MergeTypedResults(
         TMeasureType measureType,
         IFieldInfo[] fields,
         GroupMethod groupMethod,
         Sort sort,
         Dictionary<TagCollection, List<TypedReadResult<TKey, TEntry, TMeasureType>>> collectionsByTags )
      {
         // construct final result from previously grouped results
         var aggregate = FindAggregationMethod( groupMethod );
         var finalResults = new Dictionary<TagCollection, TaggedReadResult<TEntry, TMeasureType>>();
         foreach( var collections in collectionsByTags )
         {
            List<TEntry> newCollection;
            if( collections.Value.Count == 1 )
            {
               newCollection = collections.Value[ 0 ].Entries;
            }
            else
            {
               newCollection = MergeSort.Sort(
                  collections: collections.Value.Select( x => x.Entries ),
                  comparer: EntryComparer.GetComparer<TKey, TEntry>( sort ),
                  resolveConflict: x => aggregate( x, fields ) );
            }

            // need tag information RIGHT HERE
            var tagCollection = collections.Key;
            finalResults.Add( tagCollection, new TaggedReadResult<TEntry, TMeasureType>( measureType, tagCollection, sort, newCollection ) );
         }
         var finalResult = new MultiTaggedReadResult<TEntry, TMeasureType>( finalResults );
         return finalResult;
      }

      private MultiTaggedReadResult<TEntry, TMeasureType> Merge( 
         TMeasureType measureType,
         IFieldInfo[] fields,
         GroupMethod groupMethod,
         Sort sort,
         Dictionary<TagCollection, List<TaggedReadResult<TEntry, TMeasureType>>> collectionsByTags )
      {
         // construct final result from previously grouped results
         var aggregate = FindAggregationMethod( groupMethod );
         var finalResults = new Dictionary<TagCollection, TaggedReadResult<TEntry, TMeasureType>>();
         foreach( var collections in collectionsByTags )
         {
            List<TEntry> newCollection;
            if( collections.Value.Count == 1 )
            {
               newCollection = collections.Value[ 0 ].Entries;
            }
            else
            {
               newCollection = MergeSort.Sort(
                  collections: collections.Value.Select( x => x.Entries ),
                  comparer: EntryComparer.GetComparer<TKey, TEntry>( sort ),
                  resolveConflict: x => aggregate( x, fields ) );
            }

            // need tag information RIGHT HERE
            var tagCollection = collections.Key;
            finalResults.Add( tagCollection, new TaggedReadResult<TEntry, TMeasureType>( measureType, tagCollection, sort, newCollection ) );
         }
         var finalResult = new MultiTaggedReadResult<TEntry, TMeasureType>( finalResults );
         return finalResult;
      }

      private Func<IEnumerable<TEntry>, IFieldInfo[], TEntry> FindAggregationMethod( GroupMethod groupMethod )
      {
         switch( groupMethod )
         {
            case GroupMethod.Average:
               return Average;
            case GroupMethod.Sum:
               return Sum;
            case GroupMethod.Min:
               return Min;
            case GroupMethod.Max:
               return Max;
            default:
               throw new ArgumentException( "Invalid group method specified.", nameof( groupMethod ) );
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

      private static dynamic[] CreateDefaultValues( IFieldInfo[] fields )
      {
         dynamic[] array = new dynamic[ fields.Length ];
         for( int i = 0 ; i < fields.Length ; i++ )
         {
            array[ i ] = TypeHelper.GetDefaultValue( fields[ i ].ValueType );
         }
         return array;
      }

      private static TEntry Max( IEnumerable<TEntry> entries, IFieldInfo[] fields )
      {
         // we need count and for each field
         int count = 0;
         DateTime timestamp = default( DateTime );
         bool firstIteration = true;

         // calculate
         dynamic[] maxs = new dynamic[ fields.Length ];

         int fieldLen = fields.Length;
         foreach( var entry in entries )
         {
            if( firstIteration )
            {
               timestamp = entry.GetTimestamp();
               firstIteration = false;
            }
            count += entry.GetCount();

            for( int i = 0 ; i < fieldLen ; i++ )
            {
               var field = fields[ i ];
               dynamic value = entry.GetField( field.Key );

               var max = maxs[ i ];
               if( max == null || value > max )
               {
                  maxs[ i ] = value;
               }
            }
         }

         // create result
         TEntry newEntry = new TEntry();
         newEntry.SetCount( count );
         newEntry.SetTimestamp( timestamp );

         for( int i = 0 ; i < fieldLen ; i++ )
         {
            var field = fields[ i ];
            newEntry.SetField( field.Key, maxs[ i ] );
         }

         return newEntry;
      }

      private static TEntry Min( IEnumerable<TEntry> entries, IFieldInfo[] fields )
      {
         // we need count and for each field
         int count = 0;
         DateTime timestamp = default( DateTime );
         bool firstIteration = true;

         // calculate
         dynamic[] mins = new dynamic[ fields.Length ];

         int fieldLen = fields.Length;
         foreach( var entry in entries )
         {
            if( firstIteration )
            {
               timestamp = entry.GetTimestamp();
               firstIteration = false;
            }
            count += entry.GetCount();

            for( int i = 0 ; i < fieldLen ; i++ )
            {
               var field = fields[ i ];
               dynamic value = entry.GetField( field.Key );

               var min = mins[ i ];
               if( min == null || value < min )
               {
                  mins[ i ] = value;
               }
            }
         }

         // create result
         TEntry newEntry = new TEntry();
         newEntry.SetCount( count );
         newEntry.SetTimestamp( timestamp );

         for( int i = 0 ; i < fieldLen ; i++ )
         {
            var field = fields[ i ];
            newEntry.SetField( field.Key, mins[ i ] );
         }

         return newEntry;
      }

      private static TEntry Sum( IEnumerable<TEntry> entries, IFieldInfo[] fields )
      {
         // we need count and for each field
         int count = 0;
         DateTime timestamp = default( DateTime );
         bool firstIteration = true;

         // calculate
         dynamic[] sums = CreateDefaultValues( fields );

         int fieldLen = fields.Length;
         foreach( var entry in entries )
         {
            if( firstIteration )
            {
               timestamp = entry.GetTimestamp();
               firstIteration = false;
            }
            count += entry.GetCount();

            for( int i = 0 ; i < fieldLen ; i++ )
            {
               var field = fields[ i ];
               dynamic value = entry.GetField( field.Key );
               sums[ i ] += value;
            }
         }

         // create result
         TEntry newEntry = new TEntry();
         newEntry.SetCount( count );
         newEntry.SetTimestamp( timestamp );

         for( int i = 0 ; i < fieldLen ; i++ )
         {
            var field = fields[ i ];
            newEntry.SetField( field.Key, sums[ i ] );
         }

         return newEntry;
      }

      private static TEntry Average( IEnumerable<TEntry> entries, IFieldInfo[] fields )
      {
         // we need count and for each field
         int count = 0;
         DateTime timestamp = default( DateTime );
         bool firstIteration = true;

         // calculate
         dynamic[] sums = CreateDefaultValues( fields );

         int fieldLen = fields.Length;
         foreach( var entry in entries )
         {
            if( firstIteration )
            {
               timestamp = entry.GetTimestamp();
               firstIteration = false;
            }
            var entryCount = entry.GetCount();
            count += entryCount;

            for( int i = 0 ; i < fieldLen ; i++ )
            {
               var field = fields[ i ];
               dynamic value = entry.GetField( field.Key );
               sums[ i ] += value * entryCount;
            }
         }

         // create result
         TEntry newEntry = new TEntry();
         newEntry.SetCount( count );
         newEntry.SetTimestamp( timestamp );

         for( int i = 0 ; i < fieldLen ; i++ )
         {
            var field = fields[ i ];
            newEntry.SetField( field.Key, sums[ i ] / count );
         }

         return newEntry;
      }
   }
}
