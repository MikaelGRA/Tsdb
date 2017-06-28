using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb.Client
{
   public class AggregationTsdbClient<TKey, TEntry, TMeasureType> : ITypedStorage<TEntry, TMeasureType>
      where TEntry : IAggregatableEntry, new()
      where TMeasureType : IMeasureType
   {
      private readonly ITypedKeyStorage<TKey, TMeasureType> _typedKeyStorage;
      private readonly IStorageSelector<TKey, TEntry> _storageSelector;
      private readonly ITsdbLogger _logger;

      public AggregationTsdbClient( IStorageSelector<TKey, TEntry> storageSelector, ITypedKeyStorage<TKey, TMeasureType> typedKeyStorage, ITsdbLogger logger )
      {
         _typedKeyStorage = typedKeyStorage;
         _storageSelector = storageSelector;
         _logger = logger;
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync(
         string measureTypeName,
         IEnumerable<KeyValuePair<string, string>> requiredTags,
         IEnumerable<string> groupByTags,
         GroupMethod groupMethod,
         Sort sort = Sort.Descending )
      {
         var groupByTagsList = groupByTags.ToList(); // only iterate once
         var requiredTagsDictionary = requiredTags.ToDictionary( x => x.Key, x => x.Value );

         // get the type information for each key
         var typedKeys = await _typedKeyStorage.GetTaggedKeysAsync( measureTypeName, requiredTagsDictionary ).ConfigureAwait( false );

         // get results per storage
         var tasks = LookupDynamicStorages( typedKeys ).Select( c => ReadGroupsForStoreAsync( c.Storage, c.Lookups, measureTypeName, requiredTagsDictionary, groupByTagsList, groupMethod, sort ) ).ToList();

         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // get list of results
         var results = tasks.Select( x => x.Result ).ToList();

         // perform final merging
         return await MergeTaggedResultsAsync( measureTypeName, groupMethod, sort, results );
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync(
         string measureTypeName,
         DateTime to,
         IEnumerable<KeyValuePair<string, string>> requiredTags,
         IEnumerable<string> groupByTags,
         GroupMethod groupMethod,
         Sort sort = Sort.Descending )
      {
         var groupByTagsList = groupByTags.ToList(); // only iterate once
         var requiredTagsDictionary = requiredTags.ToDictionary( x => x.Key, x => x.Value );

         // get the type information for each key
         var typedKeys = await _typedKeyStorage.GetTaggedKeysAsync( measureTypeName, requiredTagsDictionary ).ConfigureAwait( false );

         // get results per storage
         var tasks = LookupDynamicStorages( typedKeys, to ).Select( c => ReadGroupsForStoreAsync( c.Storage, c.To.Value, c.Lookups, measureTypeName, requiredTagsDictionary, groupByTagsList, groupMethod, sort ) ).ToList();

         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // get list of results
         var results = tasks.Select( x => x.Result ).ToList();

         // perform final merging
         return await MergeTaggedResultsAsync( measureTypeName, groupMethod, sort, results );
      }

      public async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsAsync(
         string measureTypeName,
         DateTime from,
         DateTime to,
         IEnumerable<KeyValuePair<string, string>> requiredTags,
         IEnumerable<string> groupByTags,
         GroupMethod groupMethod,
         Sort sort = Sort.Descending )
      {
         var groupByTagsList = groupByTags.ToList(); // only iterate once
         var requiredTagsDictionary = requiredTags.ToDictionary( x => x.Key, x => x.Value );

         // get the type information for each key
         var typedKeys = await _typedKeyStorage.GetTaggedKeysAsync( measureTypeName, requiredTagsDictionary ).ConfigureAwait( false );

         // get results per storage
         var tasks = LookupDynamicStorages( typedKeys, from, to ).Select( c => ReadGroupsForStoreAsync( c.Storage, c.From.Value, c.To.Value, c.Lookups, measureTypeName, requiredTagsDictionary, groupByTagsList, groupMethod, sort ) ).ToList();

         await Task.WhenAll( tasks ).ConfigureAwait( false );

         // get list of results
         var results = tasks.Select( x => x.Result ).ToList();

         // perform final merging
         return await MergeTaggedResultsAsync( measureTypeName, groupMethod, sort, results );
      }

      private Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsForStoreAsync(
         IStorage<TKey, TEntry> storage,
         IEnumerable<ITypedKey<TKey, TMeasureType>> typedKeys,
         string measureTypeName,
         Dictionary<string, string> requiredTags,
         List<string> groupByTagsList,
         GroupMethod groupMethod,
         Sort sort )
      {
         var typedStorage = storage as ITypedStorage<TEntry, TMeasureType>;
         if( typedStorage != null )
         {
            return typedStorage.ReadGroupsAsync( measureTypeName, requiredTags, groupByTagsList, groupMethod, sort );
         }
         else
         {
            return ReadGroupsForUnsupportedStoreAsync( storage, null, null, typedKeys, groupByTagsList, groupMethod, sort );
         }
      }

      private Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsForStoreAsync(
         IStorage<TKey, TEntry> storage,
         DateTime to,
         IEnumerable<ITypedKey<TKey, TMeasureType>> typedKeys,
         string measureTypeName,
         Dictionary<string, string> requiredTags,
         List<string> groupByTagsList,
         GroupMethod groupMethod,
         Sort sort )
      {
         var typedStorage = storage as ITypedStorage<TEntry, TMeasureType>;
         if( typedStorage != null )
         {
            return typedStorage.ReadGroupsAsync( measureTypeName, to, requiredTags, groupByTagsList, groupMethod, sort );
         }
         else
         {
            return ReadGroupsForUnsupportedStoreAsync( storage, null, to, typedKeys, groupByTagsList, groupMethod, sort );
         }
      }

      private Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsForStoreAsync(
         IStorage<TKey, TEntry> storage,
         DateTime from,
         DateTime to,
         IEnumerable<ITypedKey<TKey, TMeasureType>> typedKeys,
         string measureTypeName,
         Dictionary<string, string> requiredTags,
         List<string> groupByTagsList,
         GroupMethod groupMethod,
         Sort sort )
      {
         var typedStorage = storage as ITypedStorage<TEntry, TMeasureType>;
         if( typedStorage != null )
         {
            return typedStorage.ReadGroupsAsync( measureTypeName, from, to, requiredTags, groupByTagsList, groupMethod, sort );
         }
         else
         {
            return ReadGroupsForUnsupportedStoreAsync( storage, from, to, typedKeys, groupByTagsList, groupMethod, sort );
         }
      }

      private async Task<MultiTaggedReadResult<TEntry, TMeasureType>> ReadGroupsForUnsupportedStoreAsync(
         IStorage<TKey, TEntry> storage,
         DateTime? from,
         DateTime? to,
         IEnumerable<ITypedKey<TKey, TMeasureType>> typedKeys,
         List<string> groupByTagsList,
         GroupMethod groupMethod,
         Sort sort )
      {
         // create lookup dictionary for keys to typed keys
         var lookups = typedKeys.ToDictionary( x => x.Key );
         var keys = lookups.Keys.ToList();
         var measureType = lookups.First().Value.GetMeasureType();
         var fields = measureType.GetFields().ToArray();

         // get 'traditional results'
         MultiReadResult<TKey, TEntry> result;
         if( from.HasValue && to.HasValue )
         {
            result = await storage.ReadAsync( keys, from.Value, to.Value, sort ).ConfigureAwait( false );
         }
         else if( to.HasValue )
         {
            result = await storage.ReadAsync( keys, to.Value, sort ).ConfigureAwait( false );
         }
         else
         {
            result = await storage.ReadAsync( keys, sort ).ConfigureAwait( false );
         }

         // clear out results with no entries
         result.ClearEmptyResults();

         // change into result with tagged keys
         var typedResults = result.WithTags( lookups );

         // perform grouping (GroupByTags())
         var groupedResults = GroupByTags( groupByTagsList, typedResults );

         // construct final result from previously grouped results
         return MergeTypedResults( measureType, fields, groupMethod, sort, groupedResults );
      }

      private async Task<MultiTaggedReadResult<TEntry, TMeasureType>> MergeTaggedResultsAsync(
         string measureTypeName,
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
               measureType = multiTaggedReadResult.MeasureType;

               foreach( var taggedReadResult in multiTaggedReadResult )
               {
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
               measureType = await _typedKeyStorage.GetMeasureTypeAsync( measureTypeName );
               return new MultiTaggedReadResult<TEntry, TMeasureType>( measureType );
            }
         }
      }

      private Dictionary<TagCollection, List<TypedReadResult<TKey, TEntry, TMeasureType>>> GroupByTags(
         List<string> sortedGroupByTagsList,
         MultiTypedReadResult<TKey, TEntry, TMeasureType> typedResults )
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
            finalResults.Add( tagCollection, new TaggedReadResult<TEntry, TMeasureType>( tagCollection, sort, newCollection ) );
         }
         var finalResult = new MultiTaggedReadResult<TEntry, TMeasureType>( measureType, finalResults );
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
            finalResults.Add( tagCollection, new TaggedReadResult<TEntry, TMeasureType>( tagCollection, sort, newCollection ) );
         }
         var finalResult = new MultiTaggedReadResult<TEntry, TMeasureType>( measureType, finalResults );
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

      private IEnumerable<StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>> LookupDynamicStorages( IEnumerable<ITypedKey<TKey, TMeasureType>> taggedIds )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>, StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>>();

         foreach( var id in taggedIds )
         {
            var storages = _storageSelector.GetStorage( id.Key, null, null );
            foreach( var storage in storages )
            {
               StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  existingStorage = new StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>( storage.Storage );
                  existingStorage.Lookups = new List<ITypedKey<TKey, TMeasureType>>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>> LookupDynamicStorages( IEnumerable<ITypedKey<TKey, TMeasureType>> taggedIds, DateTime to )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>, StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>>();

         foreach( var id in taggedIds )
         {
            var storages = _storageSelector.GetStorage( id.Key, null, to );
            foreach( var storage in storages )
            {
               StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry> existingStorage;
               if( !result.TryGetValue( storage, out existingStorage ) )
               {
                  var actualTo = to;
                  if( storage.To < to )
                  {
                     actualTo = storage.To.Value;
                  }

                  existingStorage = new StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>( storage.Storage, null, actualTo );
                  existingStorage.Lookups = new List<ITypedKey<TKey, TMeasureType>>();
                  result.Add( storage, existingStorage );
               }

               existingStorage.Lookups.Add( id );
            }
         }

         return result.Values;
      }

      private IEnumerable<StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>> LookupDynamicStorages( IEnumerable<ITypedKey<TKey, TMeasureType>> taggedIds, DateTime from, DateTime to )
      {
         var result = new Dictionary<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>, StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>>();

         foreach( var id in taggedIds )
         {
            var storages = _storageSelector.GetStorage( id.Key, from, to );
            foreach( var storage in storages )
            {
               StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry> existingStorage;
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

                  existingStorage = new StorageLookupResult<TKey, List<ITypedKey<TKey, TMeasureType>>, TEntry>( storage.Storage, actualFrom, actualTo );
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
            newEntry.SetField( field.Key, (object)maxs[ i ] );
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
            newEntry.SetField( field.Key, (object)mins[ i ] );
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
            newEntry.SetField( field.Key, (object)sums[ i ] );
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
            newEntry.SetField( field.Key, (object)( sums[ i ] / count ) );
         }

         return newEntry;
      }
   }
}
